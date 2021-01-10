//  Copyright (c) 2011-present, YrocksDb, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "compaction_picker_hybrid.h"

#include "logging/event_logger.h"

namespace ROCKSDB_NAMESPACE {

#define kRearangeCompaction (CompactionReason::kFIFOTtl)

HybridCompactionPicker::HybridCompactionPicker(
    const ImmutableOptions& ioptions, const InternalKeyComparator* icmp)
    : CompactionPicker(ioptions, icmp),
      curNumOfHyperLevels_(0),
      maxNumHyperLevels_(s_minNumHyperLevels),
      lastLevelThreads_(2),
      lastLevelSizeCompactionStart_(0),
      enableLow_(false),
      spaceAmpFactor_(0),
      ucmp_(icmp->user_comparator()) {
  // init the vectors with zeros
  for (uint hyperLevelNum = 0; hyperLevelNum <= s_maxNumHyperLevels;
       hyperLevelNum++) {
    multiplier_[hyperLevelNum] = s_minLevelsToMerge;
    sizeToCompact_[hyperLevelNum] = 0;
  }
}

void HybridCompactionPicker::BuildCompactionDescriptors(
    HybridComactionsDescribtors& out) const {
  for (uint i = 0; i < out.size(); i++) {
    out[i].nCompactions = 0;
    out[i].hasRearange = false;
    out[i].startLevel = -1u;
  }
  out.rearangeRunning = false;
  out[0].nCompactions = level0_compactions_in_progress()->size();

  auto compactionInProgress = compactions_in_progress();
  for (auto const& compact : *compactionInProgress) {
    uint startLevel = compact->start_level();
    if (startLevel != 0) {
      auto hyperLevelNum = GetHyperLevelNum(startLevel);
      out[hyperLevelNum].nCompactions++;
      out[hyperLevelNum].startLevel = startLevel;
      if (compact->compaction_reason() == kRearangeCompaction) {
        out[hyperLevelNum].hasRearange = true;
        out.rearangeRunning = true;
      }
    }
  }
}

bool HybridCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (curNumOfHyperLevels_ == 0) {
    return true;  // init
  }

  HybridComactionsDescribtors runningDesc(curNumOfHyperLevels_ + 1);
  BuildCompactionDescriptors(runningDesc);

  // check needs to rearange/compact on levels
  for (uint hyperLevelNum = 0; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    bool rearangeNeeded = LevelNeedsRearange(hyperLevelNum, vstorage);
    if (MayRunRearange(hyperLevelNum, runningDesc) && rearangeNeeded) {
      return true;
    }
    if (!rearangeNeeded &&
        MayStartLevelCompaction(hyperLevelNum, runningDesc, vstorage) &&
        NeedToRunLevelCompaction(hyperLevelNum, vstorage)) {
      return true;
    }
  }
  // reduce number of sorted run ....
  if (0 && runningDesc[0].nCompactions == 0 &&
      compactions_in_progress()->empty() &&
      vstorage->LevelFiles(0).size() + 2 < multiplier_[0]) {
    for (uint hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
         hyperLevelNum++) {
      auto l = LastLevelInHyper(hyperLevelNum);
      auto f = FirstLevelInHyper(hyperLevelNum) + 3;
      if (!prevSubCompaction_[hyperLevelNum - 1].empty() &&
          f <= prevSubCompaction_[hyperLevelNum - 1].outputLevel) {
        f = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
      }

      if (f <= l && !vstorage->LevelFiles(l).empty() &&
          CalculateHyperlevelSize(hyperLevelNum, vstorage) * spaceAmpFactor_ *
                  2 >=
              CalculateHyperlevelSize(hyperLevelNum + 1, vstorage)) {
        return true;
      }
    }
  }
  return false;
}

Compaction* HybridCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, SequenceNumber) {
  // one thread running
  static port::Mutex mutex;

  MutexLock mutexLock(&mutex);

  if (curNumOfHyperLevels_ == 0) {
    InitCf(mutable_cf_options, vstorage);
    size_t curDbSize = sizeToCompact_[curNumOfHyperLevels_] * spaceAmpFactor_;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Hybrid: init %u %u %lu \n",
                     cf_name.c_str(), curNumOfHyperLevels_, maxNumHyperLevels_,
                     curDbSize);
  }

  lastLevelThreads_ =
      LastLevelThreadsNum(mutable_cf_options.compaction_options_universal
                              .max_size_amplification_percent);

  HybridComactionsDescribtors runningDesc(curNumOfHyperLevels_ + 1);
  BuildCompactionDescriptors(runningDesc);
  if (runningDesc[0].nCompactions == 0) {
    prevSubCompaction_[0].setEmpty();
  }

  // rearange first
  for (uint hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    if (runningDesc[hyperLevelNum].nCompactions == 0 &&
        prevSubCompaction_[hyperLevelNum].lastKey.empty()) {
      prevSubCompaction_[hyperLevelNum].setEmpty();
    }

    bool rearangeNeeded = LevelNeedsRearange(hyperLevelNum, vstorage);

    if (rearangeNeeded && MayRunRearange(hyperLevelNum, runningDesc)) {
      Compaction* ret =
          RearangeLevel(hyperLevelNum, cf_name, mutable_cf_options,
                        mutable_db_options, vstorage);
      if (ret) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Hybrid: rearanging  hyper level %u Level %d to level %d\n",
            cf_name.c_str(), hyperLevelNum, ret->start_level(),
            ret->output_level());

        RegisterCompaction(ret);
        return ret;
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer, "[%s] Hybrid:  hyper level %u build rearange failed \n",
            cf_name.c_str(), hyperLevelNum);
      }
    }
  }

  // check db size to see if we need to move to upper level
  if (MayRunCompaction(curNumOfHyperLevels_, runningDesc) &&
      !runningDesc.rearangeRunning) {
    Compaction* ret = CheckDbSize(cf_name, mutable_cf_options,
                                  mutable_db_options, vstorage, log_buffer);
    if (ret) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Hybrid: compacting moving to level %d\n",
                       cf_name.c_str(), ret->output_level());
      RegisterCompaction(ret);
      return ret;
    }
    if (curNumOfHyperLevels_ > 1 &&
        MayRunCompaction(curNumOfHyperLevels_ - 1, runningDesc)) {
      auto lastLevelInPrevHyper = LastLevelInHyper(curNumOfHyperLevels_ - 1);
      auto dbSize = vstorage->NumLevelBytes(LastLevel());
      auto levelSize = vstorage->NumLevelBytes(lastLevelInPrevHyper);
      if (levelSize * multiplier_[curNumOfHyperLevels_] * spaceAmpFactor_ >
          dbSize) {
        ret = MoveSstToLastLevel(cf_name, mutable_cf_options,
                                 mutable_db_options, vstorage, log_buffer);
        if (ret) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Hybrid: moving large sst (%lu) db (%lu) from "
                           "%d to level %d\n",
                           cf_name.c_str(), levelSize / 1024 / 1024,
                           dbSize / 1024 / 1024, lastLevelInPrevHyper,
                           ret->output_level());
          RegisterCompaction(ret);
          return ret;
        }
      }
    }
  }

  // normal compaction start with L0
  if (MayStartLevelCompaction(0, runningDesc, vstorage)) {
    if (vstorage->LevelFiles(0).size() >= (uint)multiplier_[0]) {
      Compaction* ret = PickLevel0Compaction(
          mutable_cf_options, mutable_db_options, vstorage, multiplier_[0]);
      if (ret) {
        ROCKS_LOG_BUFFER(log_buffer, "[%s] Hybrid: compacting L0 to level %d\n",
                         cf_name.c_str(), ret->output_level());
        RegisterCompaction(ret);
        return ret;
      }
    }
  }

  for (uint hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    bool rearangeNeeded = LevelNeedsRearange(hyperLevelNum, vstorage);
    if (!rearangeNeeded &&
        MayStartLevelCompaction(hyperLevelNum, runningDesc, vstorage) &&
        NeedToRunLevelCompaction(hyperLevelNum, vstorage)) {
      Compaction* ret = PickLevelCompaction(hyperLevelNum, mutable_cf_options,
                                            mutable_db_options, vstorage);
      if (ret) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Hybrid: compacting  hyper level %u Level %d to level %d\n",
            cf_name.c_str(), hyperLevelNum, ret->start_level(),
            ret->output_level());
        RegisterCompaction(ret);
        return ret;
      } else {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Hybrid:  hyper level %u build compact failed \n",
                         cf_name.c_str(), hyperLevelNum);
      }
    }
  }

  // no compaction check for reduction
  if (0 && vstorage->LevelFiles(0).size() + 2 < multiplier_[0] &&
      runningDesc[0].nCompactions == 0 && compactions_in_progress()->empty()) {
    uint maxH = 0;
    for (uint hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
         hyperLevelNum++) {
      auto l = LastLevelInHyper(hyperLevelNum);
      auto f = FirstLevelInHyper(hyperLevelNum);
      if (!prevSubCompaction_[hyperLevelNum - 1].empty()) {
        f = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
      }

      if (f <= l && !vstorage->LevelFiles(l).empty() &&
          CalculateHyperlevelSize(hyperLevelNum, vstorage) * spaceAmpFactor_ *
                  2 >
              CalculateHyperlevelSize(hyperLevelNum + 1, vstorage)) {
        maxH = hyperLevelNum;
      }
    }
    if (maxH) {
      Compaction* ret = PickLevelCompaction(maxH, mutable_cf_options,
                                            mutable_db_options, vstorage, true);
      if (ret) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Hybrid: compact level %u Level %d to level %d "
                         "to reduce num levels\n",
                         cf_name.c_str(), maxH, ret->start_level(),
                         ret->output_level());
        RegisterCompaction(ret);

        return ret;
      }
    }
  }

  ROCKS_LOG_BUFFER(log_buffer, "[%s] Hybrid: nothing to do\n", cf_name.c_str());

  return nullptr;
}

// rearange is using compaction to move files  and hints to the compaction
// that this is a trivial move
Compaction* HybridCompactionPicker::RearangeLevel(
    uint hyperLevelNum, const std::string&,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage) {
  uint firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
  uint lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  if (!prevSubCompaction_[hyperLevelNum - 1].empty()) {
    firstLevelInHyper = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
    if (firstLevelInHyper >= lastLevelInHyper) {
      return 0;
    }
  }

  for (uint outputLevel = lastLevelInHyper; outputLevel > firstLevelInHyper;
       outputLevel--) {
    if (vstorage->LevelFiles(outputLevel).empty()) {
      std::vector<CompactionInputFiles> inputs;

      // if the level is empty move levels above to it...
      for (uint inputLevel = firstLevelInHyper; inputLevel < outputLevel;
           inputLevel++) {
        if (!vstorage->LevelFiles(inputLevel).empty()) {
          inputs.push_back(CompactionInputFiles());
          inputs.back().level = inputLevel;
          inputs.back().files = vstorage->LevelFiles(inputLevel);
        }
      }
      if (inputs.empty()) {
        return 0;
      }

      auto c = new Compaction(
          vstorage, ioptions_, mutable_cf_options, mutable_db_options,
          std::move(inputs), outputLevel, -1LL,
          /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
          GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                             outputLevel, 1),
          GetCompressionOptions(mutable_cf_options, vstorage, outputLevel),
          /* max_subcompactions */ 1, /* grandparents */ {},
          /* is manual */ false, 0, false /* deletion_compaction */,
          kRearangeCompaction);
      c->set_is_trivial_move(true);
      return c;
    }
  }
  return 0;
}

void HybridCompactionPicker::InitCf(const MutableCFOptions& mutable_cf_options,
                                    VersionStorageInfo* vstorage) {
  uint lastNonEmpty = 0;
  lastLevelSizeCompactionStart_ = 0;
  uint space_amp = mutable_cf_options.compaction_options_universal
                       .max_size_amplification_percent;
  assert(space_amp >= 110 && space_amp <= 200);
  spaceAmpFactor_ = 100.0 / (space_amp - 100);

  maxNumHyperLevels_ =
      std::max((uint)s_minNumHyperLevels,
               (uint)GetHyperLevelNum(vstorage->num_levels() - 2));
  for (uint level = 0; level < (uint)vstorage->num_levels(); level++) {
    if (!vstorage->LevelFiles(level).empty()) {
      lastNonEmpty = level;
    }
  }
  if (lastNonEmpty == 0) {
    curNumOfHyperLevels_ = s_minNumHyperLevels;
  } else {
    // assume the data is in the last level
    curNumOfHyperLevels_ = std::max((uint)s_minNumHyperLevels,
                                    (uint)GetHyperLevelNum(lastNonEmpty - 1));
  }

  size_t requiredMult =
      mutable_cf_options.compaction_options_universal.min_merge_width;
  if (requiredMult < s_minLevelsToMerge || requiredMult > s_maxLevelsToMerge) {
    requiredMult = s_maxLevelsToMerge;
  }

  size_t sizeToCompact = mutable_cf_options.write_buffer_size;
  for (uint hyperLevelNum = 0; hyperLevelNum < s_maxNumHyperLevels;
       hyperLevelNum++) {
    multiplier_[hyperLevelNum] = requiredMult;
    sizeToCompact *= multiplier_[hyperLevelNum];
    sizeToCompact_[hyperLevelNum] = sizeToCompact;
  }
}

Compaction* HybridCompactionPicker::CheckDbSize(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  // find the last level that has data
  uint lastNonEmpty = LastLevel();

  size_t actualDbSize = vstorage->NumLevelBytes(lastNonEmpty);
  if (actualDbSize == 0) {
    return nullptr;
  }

  auto spaceAmp = spaceAmpFactor_ < 1.3 ? 1.3 : spaceAmpFactor_;
  if (actualDbSize > sizeToCompact_[curNumOfHyperLevels_] * spaceAmp) {
    size_t lastHyperLevelSize =
        CalculateHyperlevelSize(curNumOfHyperLevels_, vstorage);
    auto firstLevel = FirstLevelInHyper(curNumOfHyperLevels_);

    if (actualDbSize > sizeToCompact_[curNumOfHyperLevels_] * spaceAmp * 1.2 ||
        (lastHyperLevelSize * spaceAmp < actualDbSize &&
         !vstorage->LevelFiles(firstLevel + 3).empty()) ||
        !vstorage->LevelFiles(firstLevel + 1).empty()) {
      curNumOfHyperLevels_++;
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Hybrid: increasing supported db size to %lu "
                       "requested %luM  (maxlevel is %lu):",
                       cf_name.c_str(), actualDbSize / 1024 / 1024,
                       lastHyperLevelSize / 1024 / 1024, curNumOfHyperLevels_);

      std::vector<CompactionInputFiles> inputs(1);
      inputs[0].level = lastNonEmpty;
      inputs[0].files = vstorage->LevelFiles(lastNonEmpty);
      auto outputLevel = LastLevel();
      prevSubCompaction_[curNumOfHyperLevels_ - 1].setEmpty();

      return new Compaction(
          vstorage, ioptions_, mutable_cf_options, mutable_db_options,
          std::move(inputs), outputLevel, -1,
          /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
          GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                             outputLevel, 1),
          GetCompressionOptions(mutable_cf_options, vstorage, outputLevel),
          /* max_subcompactions */ 1, /* grandparents */ {},
          /* is manual */ false, 0, false /* deletion_compaction */,
          kRearangeCompaction);
    }
  }
  return nullptr;
}

Compaction* HybridCompactionPicker::MoveSstToLastLevel(
    const std::string& /*cf_name*/, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/) {
  auto lastLevelInPrevHyper = LastLevelInHyper(curNumOfHyperLevels_ - 1);
  auto level = LastLevelInHyper(curNumOfHyperLevels_ - 1);
  for (; level > lastLevelInPrevHyper; level--) {
    if (vstorage->LevelFiles(level).empty()) {
      std::vector<CompactionInputFiles> inputs(1);
      inputs[0].level = lastLevelInPrevHyper;
      inputs[0].files = vstorage->LevelFiles(lastLevelInPrevHyper);
      auto outputLevel = level;

      return new Compaction(
          vstorage, ioptions_, mutable_cf_options, mutable_db_options,
          std::move(inputs), outputLevel, LLONG_MAX,
          /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
          GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                             outputLevel, 1),
          GetCompressionOptions(mutable_cf_options, vstorage, outputLevel),
          /* max_subcompactions */ 1, /* grandparents */ {},
          /* is manual */ false, 0, false /* deletion_compaction */,
          kRearangeCompaction);
    }
  }
  return nullptr;
}

// level needs rearange if there is a non empty sortedRun and after it an empty
// one...
bool HybridCompactionPicker::LevelNeedsRearange(
    uint hyperLevelNum, const VersionStorageInfo* vstorage) const {
  if (hyperLevelNum == 0) {
    return false;
  }

  uint firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
  uint lastLevelInHyper = LastLevelInHyper(hyperLevelNum);

  bool foundNonEmpty = false;
  for (uint level = firstLevelInHyper; level <= lastLevelInHyper; level++) {
    bool isEmpty = vstorage->LevelFiles(level).empty();
    if (!foundNonEmpty) {
      foundNonEmpty = !isEmpty;
    } else if (isEmpty) {
      return true;
    }
  }
  return false;
}

// level needs rearange if there is a non empty sortedRun and after it an empty
// one...
size_t HybridCompactionPicker::CalculateHyperlevelSize(
    uint hyperLevelNum, const VersionStorageInfo* vstorage) {
  const uint firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
  const uint lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  size_t ret = 0;
  for (uint level = firstLevelInHyper; level <= lastLevelInHyper; level++) {
    ret += vstorage->NumLevelBytes(level);
  }
  return ret;
}

Compaction* HybridCompactionPicker::PickLevel0Compaction(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    size_t mergeWidth) {
  // check that l0 has enough files
  size_t numFilesInL0 = vstorage->LevelFiles(0).size();
  if (numFilesInL0 < mergeWidth) {
    return nullptr;
  }

  // check that l1 has place
  const uint firstLevelInHyper = FirstLevelInHyper(1);
  if (!vstorage->LevelFiles(firstLevelInHyper).empty()) {
    return nullptr;
  }
  const uint lastLevelInHyper = LastLevelInHyper(1);
  // else find an empty level
  uint outputLevel = firstLevelInHyper;
  // find the last level  that all the levels belows are empty in the hyper
  // level
  for (uint i = firstLevelInHyper + 1; i <= lastLevelInHyper; i++) {
    if (!vstorage->LevelFiles(i).empty()) {
      break;
    } else {
      outputLevel = i;
    }
  }

  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  // normal compact of l0
  size_t maxWidth = mergeWidth * 1.5;

  if (numFilesInL0 < maxWidth) {
    inputs[0].files = vstorage->LevelFiles(0);
  } else {
    inputs[0].files.resize(maxWidth);
    auto iter = vstorage->LevelFiles(0).rbegin();
    for (; maxWidth > 0; maxWidth--, iter++) {
      inputs[0].files[maxWidth - 1] = *iter;
    }
  }

  size_t compactionOutputFileSize = LLONG_MAX;
  std::vector<FileMetaData*> grandparents;
  {
    bool intersecting = false;
    auto iter = inputs[0].files.begin();
    auto smallest = (*iter)->smallest.user_key();
    auto largest = (*iter)->largest.user_key();
    for (iter++; iter != inputs[0].files.end(); iter++) {
      if (ucmp_->Compare((*iter)->smallest.user_key(), largest) <= 0) {
        if (ucmp_->Compare((*iter)->largest.user_key(), smallest) >= 0) {
          intersecting = true;
          break;
        } else {
          smallest = (*iter)->smallest.user_key();
        }
      } else {
        largest = (*iter)->smallest.user_key();
      }
    }
    if (intersecting) {
      if (curNumOfHyperLevels_ <= 2) {
        grandparents = vstorage->LevelFiles(LastLevel());
      }
      if (grandparents.empty()) {
        compactionOutputFileSize = LLONG_MAX;
      }
    }
  }
  prevSubCompaction_[0].outputLevel = outputLevel;
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), outputLevel, compactionOutputFileSize, LLONG_MAX,
      0 /* max_grandparent_overlap_bytes */,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, outputLevel,
                         1),
      GetCompressionOptions(mutable_cf_options, vstorage, outputLevel),
      numFilesInL0 > maxWidth ? 2 : 1 /* max_subcompactions */, grandparents,
      /* is manual */ false, 0, false /* deletion_compaction */,
      CompactionReason::kLevelL0FilesNum);
}

static void buildGrandparents(std::vector<FileMetaData*>& grandparents,
                              const std::vector<FileMetaData*>& lastLevlfiles,
                              size_t desiredSize) {
  size_t accSize = 0;
  auto minSize = desiredSize * 3 / 5;
  for (auto f : lastLevlfiles) {
    accSize += f->fd.file_size;
    if (accSize > minSize) {
      grandparents.push_back(f);
      accSize = 0;
    }
  }
}

Compaction* HybridCompactionPicker::PickLevelCompaction(
    uint hyperLevelNum, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    bool lowPriority) {
  const uint lastLevelInHyper = LastLevelInHyper(hyperLevelNum);

  assert(!vstorage->LevelFiles(lastLevelInHyper).empty());
  uint outputLevel = lastLevelInHyper + 1;
  uint nSubCompactions = 1;
  size_t compactionOutputFileSize = LLONG_MAX;

  std::vector<FileMetaData*> grandparents;
  if (hyperLevelNum != curNumOfHyperLevels_) {
    // find output level
    if (!prevSubCompaction_[hyperLevelNum].empty()) {
      outputLevel = prevSubCompaction_[hyperLevelNum].outputLevel;
    } else {
      uint nextLevelEnd = LastLevelInHyper(hyperLevelNum + 1);
      while (outputLevel < nextLevelEnd &&
             vstorage->LevelFiles(outputLevel + 1).empty()) {
        outputLevel++;
      }
    }
    if (hyperLevelNum + 2 >= curNumOfHyperLevels_) {
      grandparents = vstorage->LevelFiles(LastLevel());
    }

    // rush the compaction to prevent stall
    const uint firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
    if (!vstorage->LevelFiles(firstLevelInHyper + 4).empty()) {
      nSubCompactions++;
    }
  } else {
    size_t lastHyperLevelSize =
        CalculateHyperlevelSize(hyperLevelNum, vstorage);
    size_t dbSize = vstorage->NumLevelBytes(LastLevel());
    if (dbSize == 0) {
      dbSize = (size_t)mutable_cf_options.write_buffer_size;
    }
    lastHyperLevelSize *= spaceAmpFactor_;
    if (dbSize && lastHyperLevelSize > dbSize) {
      nSubCompactions += lastHyperLevelSize * 10 / dbSize - 10;
      if (nSubCompactions > 4) {
        nSubCompactions = 4;
      }
    }
    const uint firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
    if (!vstorage->LevelFiles(firstLevelInHyper + 4).empty()) {
      nSubCompactions++;
    }
    compactionOutputFileSize =
        std::min(mutable_cf_options.target_file_size_base, dbSize / 32);
  }
  std::vector<CompactionInputFiles> inputs;
  if (!SelectNBuffers(inputs, lowPriority ? 1 : nSubCompactions * 4,
                      outputLevel, hyperLevelNum, vstorage)) {
    return nullptr;
  }
  // trivial compaction
  if (inputs.size() == 1) {
    grandparents.clear();
    compactionOutputFileSize = LLONG_MAX;
  } else if (hyperLevelNum == curNumOfHyperLevels_) {
    buildGrandparents(grandparents, inputs.back().files,
                      0 * compactionOutputFileSize);
  }

  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), outputLevel, compactionOutputFileSize, LLONG_MAX,
      /* max_grandparent_overlap_bytes */ 0,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, outputLevel,
                         1),
      GetCompressionOptions(mutable_cf_options, vstorage, outputLevel),
      /* max_subcompactions */ nSubCompactions, grandparents,
      /* is manual */ false, 0, false /* deletion_compaction */,
      CompactionReason::kLevelMaxLevelSize);
}

Compaction* HybridCompactionPicker::PickReduceNumLevels(
    LogBuffer*, uint hyperLevelNum, const std::string&,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage) {
  auto l = LastLevelInHyper(hyperLevelNum);
  auto f = FirstLevelInHyper(hyperLevelNum);
  if (hyperLevelNum == curNumOfHyperLevels_) {
    l = LastLevel();
  }

  for (; f < l; f++) {
    if (vstorage->LevelFiles(f).size()) {
      break;
    }
  }
  assert(f < l);
  std::vector<CompactionInputFiles> inputs(l - f + 1);

  inputs[0].level = f;
  size_t nFiles = std::min((size_t)vstorage->LevelFiles(f).size(), (size_t)4ul);
  auto iter = vstorage->LevelFiles(f).begin();
  for (; nFiles > 0; nFiles--, iter++) {
    inputs[0].files.push_back(*iter);
  }
  auto smallestKey = inputs[0].files.front()->smallest.user_key();
  auto largestKey = inputs[0].files.back()->largest.user_key();
  int count = 1;
  for (f++; f <= l; f++, count++) {
    inputs[count].level = f;

    auto& levelFiles = vstorage->LevelFiles(f);
    auto fl = locateFile(levelFiles, smallestKey, levelFiles.begin());
    for (; fl != levelFiles.end(); fl++) {
      if (ucmp_->Compare((*fl)->smallest.user_key(), largestKey) > 0) {
        break;
      }
      inputs[count].files.push_back(*fl);
    }
    if (!inputs[count].files.empty()) {
      auto const& smallestCand =
          inputs[count].files.front()->smallest.user_key();
      if (ucmp_->Compare(smallestCand, smallestKey) < 0) {
        smallestKey = smallestCand;
      }
      auto const& largestCand = inputs[count].files.back()->largest.user_key();
      if (ucmp_->Compare(largestCand, largestKey) > 0) {
        largestKey = largestCand;
      }
    }
  }

  size_t compactionOutputFileSize = LLONG_MAX;
  std::vector<FileMetaData*> grandparents;
  if (hyperLevelNum >= curNumOfHyperLevels_ - 1) {
    grandparents = vstorage->LevelFiles(LastLevel());
  }

  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), l, compactionOutputFileSize, LLONG_MAX,
      /* max_grandparent_overlap_bytes */ 0,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, l, 1),
      GetCompressionOptions(mutable_cf_options, vstorage, l),
      /* max_subcompactions */ 1, grandparents, /* is manual */ false, 0,
      false /* deletion_compaction */, CompactionReason::kFIFOReduceNumFiles);
}

bool HybridCompactionPicker::MayRunCompaction(
    uint hyperLevelNum, const HybridComactionsDescribtors& running) const {
  return (running[hyperLevelNum].nCompactions == 0 &&
          (hyperLevelNum == curNumOfHyperLevels_ ||
           !running[hyperLevelNum + 1].hasRearange));
}

// we can do rearange if the prev level compaction ended and there is no
// rearange currently in current level
bool HybridCompactionPicker::MayRunRearange(
    uint hyperLevelNum, const HybridComactionsDescribtors& running) const {
  return (hyperLevelNum > 0 && !running.rearangeRunning &&
          running[hyperLevelNum].nCompactions == 0);
}

bool HybridCompactionPicker::MayStartLevelCompaction(
    uint hyperLevelNum, const HybridComactionsDescribtors& running,
    const VersionStorageInfo* vstorage) const {
  if (running[hyperLevelNum].nCompactions > 0) {
    return false;
  }
  // check that there is a free target
  if (hyperLevelNum != curNumOfHyperLevels_ &&
      !vstorage->LevelFiles(LastLevelInHyper(hyperLevelNum) + 1).empty()) {
    return false;
  }
  if (running[hyperLevelNum].hasRearange) {
    return false;
  }
  return true;
}

bool HybridCompactionPicker::NeedToRunLevelCompaction(
    uint hyperLevelNum, const VersionStorageInfo* vstorage) const {
  if (hyperLevelNum == 0) {
    return vstorage->LevelFiles(0).size() >= multiplier_[0];
  }
  const uint lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  if (vstorage->LevelFiles(lastLevelInHyper).empty()) {
    return false;
  }

  int forceCompactLevel =
      LastLevelInHyper(hyperLevelNum) - multiplier_[hyperLevelNum] - 6;
  size_t maxSize = sizeToCompact_[hyperLevelNum];

  if (hyperLevelNum == curNumOfHyperLevels_) {
    maxSize = vstorage->NumLevelBytes(LastLevel()) /
              (spaceAmpFactor_ * 1.1);  // take 10 % extra
  }
  return (!vstorage->LevelFiles(forceCompactLevel).empty() ||
          CalculateHyperlevelSize(hyperLevelNum, vstorage) > maxSize);
}

std::vector<FileMetaData*>::const_iterator HybridCompactionPicker::locateFile(
    const std::vector<FileMetaData*>& filesList, const UserKey& key,
    const std::vector<FileMetaData*>::const_iterator& start) {
  auto iter = start;
  if (key.size()) {
    for (; iter != filesList.end(); iter++) {
      if (ucmp_->Compare((*iter)->largest.user_key(), key) >= 0) {
        break;
      }
    }
  }  // we are now in the right spot
  return iter;
}

void HybridCompactionPicker::selectNBufferFromFirstLevel(
    const std::vector<FileMetaData*>& levelFiles,
    const std::vector<FileMetaData*>& targetLevelFiles, uint maxNBuffers,
    std::vector<FileMetaData*>& outFiles, UserKey& smallestKey,
    UserKey& largestKey, UserKey& lowerBound, UserKey& upperBound,
    bool& lastFileWasSelected) {
  if (levelFiles.empty()) {
    return;
  }
  auto levelIter = levelFiles.begin();
  smallestKey = (*levelIter)->smallest.user_key();
  largestKey = (*levelIter)->largest.user_key();

  auto targetBegin =
      locateFile(targetLevelFiles, smallestKey, targetLevelFiles.begin());
  if (targetBegin == targetLevelFiles.end() ||
      ucmp_->Compare(largestKey, (*targetBegin)->smallest.user_key()) < 0) {
    // no intersection with upper level so insist on zero intersection to enable
    // minimum write amp (and allow parallelism)
    if (targetBegin != targetLevelFiles.end()) {
      upperBound = (*targetBegin)->smallest.user_key();
      if (targetBegin != targetLevelFiles.begin()) {
        auto prev = targetBegin;
        prev--;
        lowerBound = (*prev)->largest.user_key();
      }
    } else if (!targetLevelFiles.empty()) {
      auto prev = targetLevelFiles.back();
      lowerBound = prev->largest.user_key();
    }
  } else {
    // intersection exists so expand the selection to N buffers
    if (targetBegin != targetLevelFiles.begin()) {
      auto prev = targetBegin;
      prev--;
      lowerBound = (*prev)->largest.user_key();
    }
    auto targetEnd = locateFile(targetLevelFiles, largestKey, targetBegin);
    if (targetEnd != targetLevelFiles.end()) {
      auto count = targetEnd - targetBegin + 1;
      while (targetEnd != targetLevelFiles.end() && count < maxNBuffers) {
        targetEnd++;
        count++;
      }
      if (targetEnd != targetLevelFiles.end()) {
        upperBound = (*targetEnd)->largest.user_key();
      }
    }
  }

  // now we will take up to N files from the current level;
  for (uint i = 0; i < maxNBuffers; i++) {
    outFiles.push_back(*levelIter);
    levelIter++;
    if (levelIter == levelFiles.end() ||
        (upperBound.size() > 0 &&
         ucmp_->Compare(upperBound, (*levelIter)->largest.user_key()) < 0)) {
      break;
    }
  }
  largestKey = outFiles.back()->largest.user_key();
  if (levelIter != levelFiles.end() &&
      (upperBound.size() == 0 ||
       ucmp_->Compare(upperBound, (*levelIter)->smallest.user_key()) > 0)) {
    lastFileWasSelected = false;
    upperBound = (*levelIter)->smallest.user_key();
  }
}

// get two ranges
// (smallExcluded, largeExcluded) all the keys in the selected files should be
// in the middle [smallestKey, largestKey] the slected file should contains keys
// in the range
void HybridCompactionPicker::expandSelection(
    const std::vector<FileMetaData*>& levelFiles,
    std::vector<FileMetaData*>& outFiles, UserKey& lowerBound,
    UserKey& upperBound, const UserKey& smallest, const UserKey& largest,
    bool& lastFileWasSelected) {
  // find all the files that holds data between lowerBound
  // and upperBound (openRange)

  if (levelFiles.empty()) {
    return;
  }

  // find the first file that hold smallest
  auto f = locateFile(levelFiles, smallest, levelFiles.begin());
  if (lowerBound.size()) {
    while (f != levelFiles.end() &&
           ucmp_->Compare(lowerBound, (*f)->smallest.user_key()) >= 0) {
      f++;
    }
  }

  if (f == levelFiles.end()) {
    // check lowerBound
    auto last = *levelFiles.rbegin();
    if (!lowerBound.size() ||
        ucmp_->Compare(last->largest.user_key(), lowerBound) > 0) {
      lowerBound = last->largest.user_key();
    }
  } else {
    if (f != levelFiles.begin()) {
      auto prevf = f;
      --prevf;
      if (lowerBound.size() == 0 ||
          ucmp_->Compare((*prevf)->largest.user_key(), lowerBound) > 0) {
        lowerBound = (*prevf)->largest.user_key();
      }
    }
    // we are at the spot take all the files in the range smallest largest that
    // have largestKey <= upperbound
    for (; f != levelFiles.end(); f++) {
      if ((largest.size() != 0 &&
           ucmp_->Compare((*f)->smallest.user_key(), largest) > 0) ||
          (upperBound.size() != 0 &&
           ucmp_->Compare((*f)->largest.user_key(), upperBound) >= 0)) {
        break;
      } else {
        // file is contained
        outFiles.push_back(*f);
      }
    }

    // setup the large borders
    if (f != levelFiles.end() &&
        (upperBound.size() == 0 ||
         ucmp_->Compare((*f)->smallest.user_key(), upperBound) < 0)) {
      upperBound = (*f)->smallest.user_key();
      lastFileWasSelected = false;
    }
  }
}

// currently only on the last level
bool HybridCompactionPicker::SelectNBuffers(
    std::vector<CompactionInputFiles>& inputs, uint nBuffers, uint outputLevel,
    uint hyperLevelNum, VersionStorageInfo* vstorage) {
  // go down start with last level
  uint startLevel = LastLevelInHyper(hyperLevelNum);
  uint firstLevel = FirstLevelInHyper(hyperLevelNum) + 3;
  if (!prevSubCompaction_[hyperLevelNum - 1].empty() &&
      firstLevel <= prevSubCompaction_[hyperLevelNum - 1].outputLevel) {
    firstLevel = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
    if (firstLevel > startLevel) {
      return false;
    }
  }

  while (startLevel > firstLevel && vstorage->LevelFiles(startLevel).empty()) {
    startLevel--;
  }
  assert(startLevel >= firstLevel);
  uint count = 0;
  for (uint s = startLevel; s >= firstLevel; s--) {
    auto& levelFiles = vstorage->LevelFiles(s);
    if (!levelFiles.empty()) {
      count++;
    }
  }

  UserKey lowerBound, upperBound;
  UserKey smallestKey, largestKey;

  // select buffers from start level
  inputs.resize(count + 1);
  count--;

  nBuffers =
      std::max(nBuffers, (uint)vstorage->LevelFiles(LastLevel()).size() / 10);

  bool lastFileWasSelected = true;
  inputs[count].level = startLevel;
  selectNBufferFromFirstLevel(vstorage->LevelFiles(startLevel),
                              vstorage->LevelFiles(LastLevel()), nBuffers,
                              inputs[count].files, smallestKey, largestKey,
                              lowerBound, upperBound, lastFileWasSelected);
  auto prevPlace = prevSubCompaction_[hyperLevelNum].lastKey;
  if (!prevPlace.empty()) {
    if (ucmp_->Compare(prevPlace, smallestKey) < 0 &&
        (lowerBound.empty() || ucmp_->Compare(prevPlace, lowerBound) > 0)) {
      lowerBound = prevPlace;
    }
  }

  for (uint level = startLevel - 1; level >= firstLevel; level--) {
    if (!vstorage->LevelFiles(level).empty()) {
      count--;
      inputs[count].level = level;
      expandSelection(vstorage->LevelFiles(level), inputs[count].files,
                      lowerBound, upperBound, smallestKey, largestKey,
                      lastFileWasSelected);
      auto& fl = inputs[count].files;
      if (!fl.empty()) {
        if (ucmp_->Compare((*fl.begin())->smallest.user_key(), smallestKey) <
            0) {
          smallestKey = (*fl.begin())->smallest.user_key();
        }
        if (ucmp_->Compare((*fl.rbegin())->largest.user_key(), largestKey) >
            0) {
          largestKey = (*fl.rbegin())->largest.user_key();
        }
      }
    }
  }
  assert(count == 0);
  count = inputs.size() - 1;
  inputs[count].level = outputLevel;
  auto& fl = vstorage->LevelFiles(outputLevel);
  uint startPlace = 0;
  for (; startPlace < fl.size(); startPlace++) {
    if (ucmp_->Compare(fl[startPlace]->largest.user_key(), smallestKey) >= 0) {
      break;
    }
  }
  for (; startPlace < fl.size(); startPlace++) {
    if (ucmp_->Compare(fl[startPlace]->smallest.user_key(), largestKey) > 0) {
      break;
    } else {
      inputs[count].files.push_back(fl[startPlace]);
    }
  }
  // trivial move ?
  // one level with data at count -1

  if (inputs[count].empty()) {
    bool trivial_move = true;
    for (uint inp = 0; inp + 2 < count; inp++) {
      if (!inputs[inp].empty()) {
        trivial_move = false;
        break;
      }
    }
    if (trivial_move) {
      inputs[0] = inputs[count - 1];
      inputs.resize(1);
    }
  }

  prevSubCompaction_[hyperLevelNum].outputLevel = outputLevel;
  if (!lastFileWasSelected) {
    prevSubCompaction_[hyperLevelNum].lastKey = upperBound;
  } else {
    prevSubCompaction_[hyperLevelNum].lastKey.clear();
  }

  return true;
}

void HybridCompactionPicker::PrintLsmState(EventLoggerStream& stream,
                                           const VersionStorageInfo* vstorage) {
  stream << "lsm_state";
  stream.StartArray();
  for (uint level = 0; level <= curNumOfHyperLevels_; ++level) {
    stream << CalculateHyperlevelSize(level, vstorage) / 1024 / 1024;
  }
  stream << vstorage->NumLevelBytes(LastLevel()) / 1024 / 1024;

  stream.EndArray();
}

}  // namespace ROCKSDB_NAMESPACE
