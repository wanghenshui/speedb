//  Copyright (c) 2011-present, YrocksDb, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "compaction_picker_hybrid.h"

#include <algorithm>

#include "logging/event_logger.h"

namespace ROCKSDB_NAMESPACE {

#define kRearangeCompaction (CompactionReason::kFIFOTtl)

constexpr size_t HybridCompactionPicker::kFilesToCompactMax;
constexpr size_t HybridCompactionPicker::kLevelsToMergeMin;
constexpr size_t HybridCompactionPicker::kLevelsToMergeMax;
constexpr size_t HybridCompactionPicker::kHyperLevelsNumMax;
constexpr size_t HybridCompactionPicker::kHyperLevelsNumMin;
constexpr size_t HybridCompactionPicker::kLevelsInHyperLevel;

HybridCompactionPicker::HybridCompactionPicker(
    const ImmutableOptions& ioptions, const InternalKeyComparator* icmp)
    : CompactionPicker(ioptions, icmp),
      mutex_(),
      curNumOfHyperLevels_(0),
      maxNumHyperLevels_(kHyperLevelsNumMin),
      lastLevelSizeCompactionStart_(0),
      level0_compaction_trigger_(kLevelsToMergeMin),
      enableLow_(false),
      spaceAmpFactor_(0),
      ucmp_(icmp->user_comparator()),
      max_open_files_(10000) {
  // init the vectors with zeros
  for (size_t hyperLevelNum = 0; hyperLevelNum <= kHyperLevelsNumMax;
       hyperLevelNum++) {
    multiplier_[hyperLevelNum] = kLevelsToMergeMin;
    sizeToCompact_[hyperLevelNum] = 0;
  }
}

void HybridCompactionPicker::BuildCompactionDescriptors(
    HybridComactionsDescribtors& out) const {
  for (auto& descriptor : out) {
    descriptor.nCompactions = 0;
    descriptor.hasRearange = false;
    descriptor.startLevel = -1u;
  }
  out.rearangeRunning = false;
  out.manualComapctionRunning = false;

  out[0].nCompactions = level0_compactions_in_progress()->size();

  auto compactionInProgress = compactions_in_progress();
  for (auto const& compact : *compactionInProgress) {
    if (compact->compaction_reason() == CompactionReason::kManualCompaction) {
      out.manualComapctionRunning = true;
    }

    size_t startLevel = compact->start_level();
    if (startLevel != 0) {
      auto hyperLevelNum = GetHyperLevelNum(startLevel);
      if (startLevel >= LastLevel()) {
        hyperLevelNum = curNumOfHyperLevels_;
      }
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

  HybridComactionsDescribtors runningDesc(curNumOfHyperLevels_ + 2);
  BuildCompactionDescriptors(runningDesc);

  if (runningDesc.manualComapctionRunning) {
    return false;
  }

  // check needs to rearange/compact on levels
  for (size_t hyperLevelNum = 0; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    bool rearangeNeeded = LevelNeedsRearange(hyperLevelNum, vstorage,
                                             FirstLevelInHyper(hyperLevelNum));
    if (MayRunRearange(hyperLevelNum, runningDesc) && rearangeNeeded) {
      return true;
    }
    if (!rearangeNeeded &&
        MayStartLevelCompaction(hyperLevelNum, runningDesc, vstorage) &&
        NeedToRunLevelCompaction(hyperLevelNum, vstorage)) {
      return true;
    }
  }

  if (vstorage->LevelFiles(int(LastLevel())).size() > max_open_files_ / 2) {
    return true;
  }
  // reduce number of sorted run ....
  // need to more than 4 levels with data
  if (0 && enableLow_ && runningDesc[0].nCompactions == 0 &&
      compactions_in_progress()->empty()) {
    if (vstorage->LevelFiles(0).size() >= level0_compaction_trigger_ / 2) {
      return true;
    }

    for (size_t hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
         hyperLevelNum++) {
      auto l = LastLevelInHyper(hyperLevelNum);
      if (!vstorage->LevelFiles(l).empty()) {
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
  MutexLock mutexLock(&mutex_);

  if (curNumOfHyperLevels_ == 0) {
    InitCf(mutable_cf_options, vstorage);
    size_t curDbSize = sizeToCompact_[curNumOfHyperLevels_] * spaceAmpFactor_;
    if (enable_spdb_log) {
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Hybrid: init %u %u %lu \n",
                       cf_name.c_str(), curNumOfHyperLevels_,
                       maxNumHyperLevels_, curDbSize);
    }
  }

  HybridComactionsDescribtors runningDesc(curNumOfHyperLevels_ + 2);
  BuildCompactionDescriptors(runningDesc);
  if (runningDesc.manualComapctionRunning) {
    return nullptr;
  }

  // rearange first
  for (size_t hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    if (runningDesc[hyperLevelNum - 1].nCompactions == 0 &&
        prevSubCompaction_[hyperLevelNum - 1].lastKey.empty()) {
      prevSubCompaction_[hyperLevelNum - 1].setEmpty();
    }

    size_t startLevel = FirstLevelInHyper(hyperLevelNum);
    if (MayRunRearange(hyperLevelNum, runningDesc) &&
        LevelNeedsRearange(hyperLevelNum, vstorage,
                           FirstLevelInHyper(hyperLevelNum))) {
      if (runningDesc[hyperLevelNum - 1].nCompactions == 0) {
        prevSubCompaction_[hyperLevelNum - 1].setEmpty();
      } else {
        startLevel = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
      }
      if (LevelNeedsRearange(hyperLevelNum, vstorage, startLevel)) {
        Compaction* ret =
            RearangeLevel(hyperLevelNum, cf_name, mutable_cf_options,
                          mutable_db_options, vstorage);
        if (ret) {
          if (enable_spdb_log) {
            ROCKS_LOG_BUFFER(log_buffer,
                             "[%s] Hybrid: rearanging  hyper level %u Level %d "
                             "to level %d\n",
                             cf_name.c_str(), hyperLevelNum, ret->start_level(),
                             ret->output_level());
          }
          RegisterCompaction(ret);
          return ret;
        } else {
          if (enable_spdb_log) {
            ROCKS_LOG_BUFFER(
                log_buffer,
                "[%s] Hybrid:  hyper level %u build rearange failed \n",
                cf_name.c_str(), hyperLevelNum);
          }
        }
      }
    }
  }

  // check db size to see if we need to move to upper level
  if (MayRunCompaction(curNumOfHyperLevels_, runningDesc) &&
      !runningDesc.rearangeRunning) {
    Compaction* ret = CheckDbSize(cf_name, mutable_cf_options,
                                  mutable_db_options, vstorage, log_buffer);
    if (ret) {
      if (enable_spdb_log) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Hybrid: compacting moving to level %d\n",
                         cf_name.c_str(), ret->output_level());
      }
      RegisterCompaction(ret);
      return ret;
    }
    if (curNumOfHyperLevels_ > 1 &&
        MayRunCompaction(curNumOfHyperLevels_ - 1, runningDesc)) {
      auto lastLevelInPrevHyper = LastLevelInHyper(curNumOfHyperLevels_ - 1);
      auto dbSize = vstorage->NumLevelBytes(int(LastLevel()));
      auto levelSize = vstorage->NumLevelBytes(int(lastLevelInPrevHyper));
      if (levelSize * multiplier_[curNumOfHyperLevels_] * spaceAmpFactor_ >
          dbSize) {
        ret = MoveSstToLastLevel(cf_name, mutable_cf_options,
                                 mutable_db_options, vstorage, log_buffer);
        if (ret) {
          if (enable_spdb_log) {
            ROCKS_LOG_BUFFER(
                log_buffer,
                "[%s] Hybrid: moving large sst (%lu) db (%lu) from "
                "%d to level %d\n",
                cf_name.c_str(), levelSize / 1024 / 1024, dbSize / 1024 / 1024,
                lastLevelInPrevHyper, ret->output_level());
          }
          RegisterCompaction(ret);
          return ret;
        }
      }
    }
  }

  // normal compaction start with L0
  if (MayStartLevelCompaction(0, runningDesc, vstorage)) {
    const size_t l0_threshold =
        std::min(level0_compaction_trigger_,
                 size_t(mutable_cf_options.level0_file_num_compaction_trigger));
    if (vstorage->LevelFiles(0).size() >= l0_threshold) {
      Compaction* ret = PickLevel0Compaction(
          mutable_cf_options, mutable_db_options, vstorage, l0_threshold);
      if (ret) {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Hybrid: compacting L0 to level %d\n",
                           cf_name.c_str(), ret->output_level());
        }
        RegisterCompaction(ret);
        return ret;
      }
    }
  }

  for (size_t hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
       hyperLevelNum++) {
    if (MayStartLevelCompaction(hyperLevelNum, runningDesc, vstorage) &&
        NeedToRunLevelCompaction(hyperLevelNum, vstorage)) {
      Compaction* ret =
          PickLevelCompaction(hyperLevelNum, mutable_cf_options,
                              mutable_db_options, vstorage, false, log_buffer);
      if (ret) {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(
              log_buffer,
              "[%s] Hybrid: compacting  hyper level %u Level %d to level %d\n",
              cf_name.c_str(), hyperLevelNum, ret->start_level(),
              ret->output_level());
        }
        RegisterCompaction(ret);
        return ret;
      } else {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(
              log_buffer,
              "[%s] Hybrid:  hyper level %u build compact failed \n",
              cf_name.c_str(), hyperLevelNum);
        }
      }
    }
  }
  if (MayStartLevelCompaction(curNumOfHyperLevels_, runningDesc, vstorage)) {
    if (vstorage->LevelFiles(int(LastLevel())).size() > max_open_files_ / 2) {
      auto dbSize = vstorage->NumLevelBytes(int(LastLevel()));
      auto ret =
          PickReduceNumFiles(mutable_cf_options, mutable_db_options, vstorage,
                             std::min(dbSize / 1024, 1ul << 28));
      if (ret) {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Hybrid: compact level %u  "
                           "to reduce num number of files\n",
                           cf_name.c_str(), ret->output_level());
        }
        RegisterCompaction(ret);
        return ret;
      }
    }
  }

  // no compaction check for reduction
  if (0 && enableLow_ && runningDesc[0].nCompactions == 0 &&
      compactions_in_progress()->empty()) {
    const size_t l0_threshold = std::min(
        multiplier_[0] / 2,
        std::min(
            level0_compaction_trigger_,
            size_t(mutable_cf_options.level0_file_num_compaction_trigger)));
    if (vstorage->LevelFiles(0).size() >= l0_threshold) {
      auto ret = PickLevel0Compaction(mutable_cf_options, mutable_db_options,
                                      vstorage, 1);
      if (ret) {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(log_buffer,
                           "[%s] Hybrid: compact level 0 to level %d "
                           "to reduce num levels\n",
                           cf_name.c_str(), ret->output_level());
        }
        RegisterCompaction(ret);
        return ret;
      }
    }
    for (size_t hyperLevelNum = 1; hyperLevelNum <= curNumOfHyperLevels_;
         hyperLevelNum++) {
      auto l = LastLevelInHyper(hyperLevelNum);
      if (!vstorage->LevelFiles(l).empty()) {
        Compaction* ret =
            PickLevelCompaction(hyperLevelNum, mutable_cf_options,
                                mutable_db_options, vstorage, true, log_buffer);
        if (ret) {
          if (enable_spdb_log) {
            ROCKS_LOG_BUFFER(
                log_buffer,
                "[%s] Hybrid: compact level %u Level %d to level %d "
                "to reduce num levels\n",
                cf_name.c_str(), hyperLevelNum, ret->start_level(),
                ret->output_level());
          }
          RegisterCompaction(ret);
          return ret;
        }
      }
    }
  }

  return nullptr;
}

// rearange is using compaction to move files  and hints to the compaction
// that this is a trivial move
Compaction* HybridCompactionPicker::RearangeLevel(
    size_t hyperLevelNum, const std::string&,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage) {
  size_t firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
  size_t lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  if (!prevSubCompaction_[hyperLevelNum - 1].empty()) {
    firstLevelInHyper = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
    if (firstLevelInHyper >= lastLevelInHyper) {
      return 0;
    }
  }

  for (int outputLevel = int(lastLevelInHyper);
       outputLevel >= int(firstLevelInHyper); outputLevel--) {
    if (vstorage->LevelFiles(outputLevel).empty()) {
      std::vector<CompactionInputFiles> inputs;

      // if the level is empty move levels above to it...
      for (size_t inputLevel = firstLevelInHyper;
           inputLevel < size_t(outputLevel); inputLevel++) {
        if (!vstorage->LevelFiles(int(inputLevel)).empty()) {
          CompactionInputFiles input;
          input.level = int(inputLevel);
          input.files = vstorage->LevelFiles(int(inputLevel));
          inputs.push_back(std::move(input));
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
  size_t lastNonEmpty = 0;
  lastLevelSizeCompactionStart_ = 0;
  uint space_amp = mutable_cf_options.compaction_options_universal
                       .max_size_amplification_percent;
  assert(space_amp >= 110 && space_amp <= 200);
  spaceAmpFactor_ = 100.0 / (space_amp - 100);

  maxNumHyperLevels_ = std::max(kHyperLevelsNumMin,
                                GetHyperLevelNum(vstorage->num_levels() - 2));
  for (size_t level = 0; level < size_t(vstorage->num_levels()); level++) {
    if (!vstorage->LevelFiles(int(level)).empty()) {
      lastNonEmpty = level;
    }
  }
  if (lastNonEmpty == 0) {
    curNumOfHyperLevels_ = kHyperLevelsNumMin;
  } else {
    // assume the data is in the last level
    curNumOfHyperLevels_ =
        std::max(kHyperLevelsNumMin, GetHyperLevelNum(lastNonEmpty - 1));
  }

  size_t requiredMult =
      mutable_cf_options.compaction_options_universal.min_merge_width;
  if (requiredMult < kLevelsToMergeMin || requiredMult > kLevelsToMergeMax) {
    requiredMult = kLevelsToMergeMax;
  }

  size_t sizeToCompact = mutable_cf_options.write_buffer_size;
  for (size_t hyperLevelNum = 0; hyperLevelNum < kHyperLevelsNumMax;
       hyperLevelNum++) {
    multiplier_[hyperLevelNum] = requiredMult;
    sizeToCompact *= multiplier_[hyperLevelNum];
    sizeToCompact_[hyperLevelNum] = sizeToCompact;
  }

  level0_compaction_trigger_ =
      std::min(multiplier_[0],
               size_t(mutable_cf_options.level0_file_num_compaction_trigger));
}

Compaction* HybridCompactionPicker::CheckDbSize(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  // find the last level that has data
  size_t lastNonEmpty = LastLevel();

  size_t actualDbSize = vstorage->NumLevelBytes(int(lastNonEmpty));
  if (actualDbSize == 0) {
    return nullptr;
  }

  auto spaceAmp = spaceAmpFactor_ < 1.3 ? 1.3 : spaceAmpFactor_;
  if (actualDbSize > sizeToCompact_[curNumOfHyperLevels_] * spaceAmp) {
    const size_t lastHyperLevelSize =
        CalculateHyperlevelSize(curNumOfHyperLevels_, vstorage);
    auto firstLevel = FirstLevelInHyper(curNumOfHyperLevels_);

    if (actualDbSize > sizeToCompact_[curNumOfHyperLevels_] * spaceAmp * 1.2 ||
        (lastHyperLevelSize * spaceAmp < actualDbSize &&
         !vstorage->LevelFiles(int(firstLevel + 3)).empty()) ||
        !vstorage->LevelFiles(int(firstLevel + 1)).empty()) {
      curNumOfHyperLevels_++;
      if (enable_spdb_log) {
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Hybrid: increasing supported db size to %lu "
                         "requested %luM  (maxlevel is %lu):",
                         cf_name.c_str(), actualDbSize / 1024 / 1024,
                         lastHyperLevelSize / 1024 / 1024,
                         curNumOfHyperLevels_);
      }

      const size_t numLevelsToMove =
          std::min(kLevelsToMergeMax * 2, lastNonEmpty - 1);
      std::vector<CompactionInputFiles> inputs;
      inputs.reserve(numLevelsToMove);
      auto level = lastNonEmpty + 1 - numLevelsToMove;
      for (size_t i = 0; i < numLevelsToMove; i++) {
        CompactionInputFiles input;
        input.level = int(level);
        input.files = vstorage->LevelFiles(int(level));
        inputs.push_back(std::move(input));
        level++;
      }
      const size_t outputLevel = LastLevel();
      prevSubCompaction_[curNumOfHyperLevels_ - 1].setEmpty();

      auto* ret = new Compaction(
          vstorage, ioptions_, mutable_cf_options, mutable_db_options,
          std::move(inputs), int(outputLevel), -1,
          /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
          GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                             int(outputLevel), 1),
          GetCompressionOptions(mutable_cf_options, vstorage, int(outputLevel)),
          /* max_subcompactions */ 1, /* grandparents */ {},
          /* is manual */ false, 0, false /* deletion_compaction */,
          kRearangeCompaction);
      ret->set_is_trivial_move(true);
      return ret;
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
    if (vstorage->LevelFiles(int(level)).empty()) {
      CompactionInputFiles input;
      input.level = int(lastLevelInPrevHyper);
      input.files = vstorage->LevelFiles(int(lastLevelInPrevHyper));
      std::vector<CompactionInputFiles> inputs{std::move(input)};
      auto outputLevel = level;

      return new Compaction(
          vstorage, ioptions_, mutable_cf_options, mutable_db_options,
          std::move(inputs), int(outputLevel), LLONG_MAX,
          /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
          GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                             int(outputLevel), 1),
          GetCompressionOptions(mutable_cf_options, vstorage, int(outputLevel)),
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
    size_t hyperLevelNum, const VersionStorageInfo* vstorage,
    size_t firstLevel) const {
  if (hyperLevelNum == 0) {
    return false;
  }

  const size_t lastLevel = LastLevelInHyper(hyperLevelNum);
  bool foundNonEmpty = false;
  for (size_t level = firstLevel; level <= lastLevel; level++) {
    bool isEmpty = vstorage->LevelFiles(int(level)).empty();
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
    size_t hyperLevelNum, const VersionStorageInfo* vstorage) {
  const size_t firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
  const size_t lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  size_t ret = 0;
  for (size_t level = firstLevelInHyper; level <= lastLevelInHyper; level++) {
    ret += vstorage->NumLevelBytes(int(level));
  }
  return ret;
}

Compaction* HybridCompactionPicker::PickLevel0Compaction(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    size_t mergeWidth) {
  const auto& level0_files = vstorage->LevelFiles(0);

  // check that l0 has enough files
  if (level0_files.size() < mergeWidth) {
    return nullptr;
  }

  // check that l1 has place
  const size_t firstLevelInHyper = FirstLevelInHyper(1);
  if (!vstorage->LevelFiles(int(firstLevelInHyper)).empty()) {
    return nullptr;
  }
  const size_t lastLevelInHyper = LastLevelInHyper(1);
  // else find an empty level
  size_t outputLevel = firstLevelInHyper;
  // find the last level  that all the levels belows are empty in the hyper
  // level
  for (size_t i = firstLevelInHyper + 1; i <= lastLevelInHyper; i++) {
    if (!vstorage->LevelFiles(int(i)).empty()) {
      break;
    } else {
      outputLevel = i;
    }
  }

  // normal compact of l0
  const size_t l0_max_width = multiplier_[0];

  CompactionInputFiles input;
  input.level = 0;
  const auto input_file_count = std::min(level0_files.size(), l0_max_width);
  input.files.reserve(input_file_count);
  input.files.insert(input.files.end(), level0_files.end() - input_file_count,
                     level0_files.end());

  std::vector<CompactionInputFiles> inputs{std::move(input)};

  size_t compactionOutputFileSize = LLONG_MAX;
  std::vector<FileMetaData*> grandparents;
  if (curNumOfHyperLevels_ <= 2) {
    grandparents = vstorage->LevelFiles(int(LastLevel()));
  }

  prevSubCompaction_[0].outputLevel = outputLevel;
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), int(outputLevel), compactionOutputFileSize, LLONG_MAX,
      0 /* max_grandparent_overlap_bytes */,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                         int(outputLevel), 1),
      GetCompressionOptions(mutable_cf_options, vstorage, int(outputLevel)),
      level0_files.size() > l0_max_width ? 2 : 1 /* max_subcompactions */,
      grandparents,
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
    size_t hyperLevelNum, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    bool lowPriority, LogBuffer* log_buffer) {
  const size_t lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  assert(!vstorage->LevelFiles(int(lastLevelInHyper)).empty());

  size_t outputLevel = lastLevelInHyper + 1;
  size_t nSubCompactions = 1;
  size_t compactionOutputFileSize = 1ul << 30;

  std::vector<FileMetaData*> grandparents;
  if (hyperLevelNum != curNumOfHyperLevels_) {
    // find output level
    size_t nextLevelEnd = LastLevelInHyper(hyperLevelNum + 1);
    while (outputLevel < nextLevelEnd &&
           vstorage->LevelFiles(int(outputLevel + 1)).empty()) {
      outputLevel++;
    }
    if (!prevSubCompaction_[hyperLevelNum].empty()) {
      auto k = vstorage->LevelFiles(int(lastLevelInHyper))
                   .back()
                   ->largest.user_key();
      const Slice last_key(prevSubCompaction_[hyperLevelNum].lastKey);
      if (ucmp_->Compare(k, last_key) > 0) {
        outputLevel = prevSubCompaction_[hyperLevelNum].outputLevel;
      }
    }

    grandparents = vstorage->LevelFiles(int(LastLevel()));
    // rush the compaction to prevent stall
    const size_t firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
    for (size_t i = 2; i < 6; i++) {
      if (!vstorage->LevelFiles(int(firstLevelInHyper + i)).empty()) {
        nSubCompactions++;
      }
    }
  } else {
    const size_t lastHyperLevelSize =
        spaceAmpFactor_ * CalculateHyperlevelSize(hyperLevelNum, vstorage);
    size_t dbSize = std::max<size_t>(vstorage->NumLevelBytes(int(LastLevel())),
                                     mutable_cf_options.write_buffer_size * 8);
    compactionOutputFileSize = std::min(compactionOutputFileSize, dbSize / 8);
    if (lastHyperLevelSize > dbSize) {
      nSubCompactions += lastHyperLevelSize * 10 / dbSize - 10;
      if (nSubCompactions > 4) {
        nSubCompactions = 4;
      }
    }
    const size_t firstLevelInHyper = FirstLevelInHyper(hyperLevelNum);
    if (!vstorage->LevelFiles(int(firstLevelInHyper + 4)).empty()) {
      nSubCompactions++;
    }
  }

  std::vector<CompactionInputFiles> inputs;
  size_t num_buffers = lowPriority ? 1 : nSubCompactions * 4;
  if (grandparents.size() / 10 > num_buffers)
    num_buffers = grandparents.size() / 10;

  if (!SelectNBuffers(inputs, num_buffers, outputLevel, hyperLevelNum, vstorage,
                      log_buffer)) {
    return nullptr;
  }

  bool trivial_compaction = false;
  if (inputs.size() == 1) {
    // inputs does not intersect with output so we can move
    grandparents.clear();
    compactionOutputFileSize = LLONG_MAX;
    trivial_compaction = true;
  } else if (hyperLevelNum == curNumOfHyperLevels_) {
    buildGrandparents(grandparents, inputs.back().files,
                      compactionOutputFileSize);
  }

  auto ret = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), int(outputLevel), compactionOutputFileSize, LLONG_MAX,
      /* max_grandparent_overlap_bytes */ 0,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                         int(outputLevel), 1),
      GetCompressionOptions(mutable_cf_options, vstorage, int(outputLevel)),
      /* max_subcompactions */ uint32_t(nSubCompactions), grandparents,
      /* is manual */ false, 0, false /* deletion_compaction */,
      CompactionReason::kLevelMaxLevelSize);
  if (trivial_compaction) {
    ret->set_is_trivial_move(true);
  }
  return ret;
}

Compaction* HybridCompactionPicker::PickReduceNumFiles(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    size_t minFileSize) {
  auto lastLevel = LastLevel();
  auto& fl = vstorage->LevelFiles(int(lastLevel));
  // find sequence of small files
  size_t maxSeq = 0;
  size_t maxSeqPlace = 0;

  for (size_t firstFile = 0; firstFile < fl.size();) {
    auto const* f = fl[firstFile];
    if (f->raw_value_size < minFileSize) {
      size_t totalSize = f->raw_value_size;
      auto i = firstFile + 1;
      for (; i < fl.size(); i++) {
        auto const* nf = fl[i];
        if (nf->raw_value_size > minFileSize) {
          break;
        }

        if (mutable_cf_options.table_prefix_size > 0) {
          const Slice smallest_prefix(nf->smallest.user_key().data(),
                                      mutable_cf_options.table_prefix_size);
          const Slice largest_prefix(nf->largest.user_key().data(),
                                     mutable_cf_options.table_prefix_size);

          if (ucmp_->Compare(smallest_prefix, largest_prefix) != 0) {
            break;
          }
        }

        totalSize += f->raw_value_size;
        if (totalSize > (1ul << 30)) {
          break;
        }
      }
      if (i - firstFile > maxSeq) {
        maxSeq = i - firstFile;
        maxSeqPlace = firstFile;
      }
      firstFile = i;
    } else {
      firstFile++;
    }
  }
  if (maxSeq <= 1) {
    return nullptr;
  }
  if (maxSeq > 200) {
    maxSeq = 200;
  }
  CompactionInputFiles input;
  input.level = int(lastLevel);
  input.files.reserve(maxSeq);
  input.files.insert(input.files.end(), fl.begin() + maxSeqPlace,
                     fl.begin() + maxSeqPlace + maxSeq);
  std::vector<CompactionInputFiles> inputs{std::move(input)};
  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), int(lastLevel), -1LL,
      /* max_grandparent_overlap_bytes */ LLONG_MAX, 0,
      GetCompressionType(ioptions_, vstorage, mutable_cf_options,
                         int(lastLevel), 1),
      GetCompressionOptions(mutable_cf_options, vstorage, int(lastLevel)),
      /* max_subcompactions */ 1, /* grandparents */ {},
      /* is manual */ false, 0, false /* deletion_compaction */,
      CompactionReason::kFIFOReduceNumFiles);
  c->set_is_trivial_move(false);
  return c;
}

bool HybridCompactionPicker::MayRunCompaction(
    size_t hyperLevelNum, const HybridComactionsDescribtors& running) const {
  return (running[hyperLevelNum].nCompactions == 0 &&
          (hyperLevelNum == curNumOfHyperLevels_ ||
           !running[hyperLevelNum + 1].hasRearange));
}

// we can do rearange if the prev level compaction ended and there is no
// rearange currently in current level
bool HybridCompactionPicker::MayRunRearange(
    size_t hyperLevelNum, const HybridComactionsDescribtors& running) const {
  return (hyperLevelNum > 0 && !running.rearangeRunning &&
          running[hyperLevelNum].nCompactions == 0);
}

bool HybridCompactionPicker::MayStartLevelCompaction(
    size_t hyperLevelNum, const HybridComactionsDescribtors& running,
    const VersionStorageInfo* vstorage) const {
  if (running[hyperLevelNum].nCompactions > 0) {
    return false;
  }
  // check that there is a free target
  if (hyperLevelNum != curNumOfHyperLevels_ &&
      prevSubCompaction_[hyperLevelNum].empty() &&
      !vstorage->LevelFiles(int(LastLevelInHyper(hyperLevelNum) + 1)).empty()) {
    return false;
  }
  return true;
}

bool HybridCompactionPicker::NeedToRunLevelCompaction(
    size_t hyperLevelNum, const VersionStorageInfo* vstorage) const {
  if (hyperLevelNum == 0) {
    return vstorage->LevelFiles(0).size() >= level0_compaction_trigger_;
  }

  auto lastLevelInHyper = LastLevelInHyper(hyperLevelNum);
  if (vstorage->LevelFiles(int(lastLevelInHyper)).empty()) {
    return false;
  }

  int forceCompactLevel =
      int(lastLevelInHyper - multiplier_[hyperLevelNum]) - 6;
  size_t maxSize = sizeToCompact_[hyperLevelNum];
  size_t levelSize = vstorage->NumLevelBytes(int(LastLevel())) /
                     (spaceAmpFactor_ * 1.1);  // take 10 % extra

  for (size_t hyperLevel = hyperLevelNum; hyperLevel < curNumOfHyperLevels_;
       hyperLevel++) {
    levelSize /= multiplier_[hyperLevel];
  }

  if (maxSize > levelSize) maxSize = levelSize;

  return (!vstorage->LevelFiles(int(forceCompactLevel)).empty() ||
          CalculateHyperlevelSize(hyperLevelNum, vstorage) > maxSize);
}

bool HybridCompactionPicker::Intersecting(
    const FileMetaData* f1, const std::vector<FileMetaData*>& f2) const {
  auto iter = locateFile(f2, f1->smallest.user_key(), f2.begin());
  return (iter != f2.end() && ucmp_->Compare((*iter)->smallest.user_key(),
                                             f1->largest.user_key()) > 0);
}

bool HybridCompactionPicker::Intersecting(
    const std::vector<FileMetaData*>& f1,
    const std::vector<FileMetaData*>& f2) const {
  for (auto* f : f1) {
    if (Intersecting(f, f2)) {
      return true;
    }
  }
  return false;
}

std::vector<FileMetaData*>::const_iterator HybridCompactionPicker::locateFile(
    const std::vector<FileMetaData*>& filesList, const Slice& key,
    const std::vector<FileMetaData*>::const_iterator& start) const {
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
    const std::vector<FileMetaData*>& targetLevelFiles, size_t maxNBuffers,
    std::vector<FileMetaData*>& outFiles, Slice& smallestKey, Slice& largestKey,
    Slice& lowerBound, Slice& upperBound, bool& lastFileWasSelected) {
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
    if (targetBegin != targetLevelFiles.begin()) {
      auto prev = targetBegin;
      prev--;
      lowerBound = (*prev)->largest.user_key();
    }
  }
  size_t currentTargetSize = 0;  // accumulate size of the target level
  size_t currentLevelSize =
      (*levelIter)->fd.file_size;  // accumulated size of the current level

  auto targetEnd = targetBegin;
  // first file that do not intersect with last
  for (; targetEnd != targetLevelFiles.end(); targetEnd++) {
    if (ucmp_->Compare((*targetEnd)->smallest.user_key(),
                       (*levelIter)->largest.user_key()) > 0) {
      break;
    }
    currentTargetSize += (*targetEnd)->fd.file_size;
  }

  bool expand = true;
  outFiles.push_back(*levelIter);
  levelIter++;

  while (levelIter != levelFiles.end() && expand) {
    if (upperBound.size() > 0 &&
        ucmp_->Compare(upperBound, (*levelIter)->largest.user_key()) < 0) {
      // expand over the upper bound
      expand = false;
      break;
    } else if (targetEnd == targetLevelFiles.end() ||
               ucmp_->Compare((*targetEnd)->smallest.user_key(),
                              (*levelIter)->largest.user_key()) > 0) {
      // Todo fix and add comment
      // "free" file check the comapction size and the write amp
      if (outFiles.size() > maxNBuffers && currentLevelSize < (1ul << 26) &&
          currentTargetSize < currentLevelSize * 2) {
        expand = false;
      }
    } else {
      // target end starts after the current file expand only if too small
      // compaction && this file is not completely excluded
      size_t newSize = currentTargetSize + (*targetEnd)->fd.file_size;
      if (outFiles.size() >= maxNBuffers ||
          ucmp_->Compare((*targetEnd)->largest.user_key(),
                         (*levelIter)->smallest.user_key()) < 0) {
        expand = false;
      } else {
        currentTargetSize = newSize;
        targetEnd++;
      }
    }
    if (expand) {
      currentLevelSize += (*levelIter)->fd.file_size;
      outFiles.push_back(*levelIter);
      levelIter++;
    }
  }

  largestKey = outFiles.back()->largest.user_key();

  // Need to check for cases where next file has the same user key with
  // a different version and select those files as well
  bool expanded_overlapping = false;
  for (; levelIter != levelFiles.end(); ++levelIter) {
    if (ucmp_->Compare(largestKey, (*levelIter)->smallest.user_key()) != 0) {
      break;
    }
    outFiles.push_back(*levelIter);
    largestKey = (*levelIter)->largest.user_key();
    expanded_overlapping = true;
  }
  if (expanded_overlapping) {
    targetEnd = locateFile(targetLevelFiles, largestKey, targetEnd);
  }

  if (targetEnd != targetLevelFiles.end()) {
    upperBound = (*targetEnd)->smallest.user_key();
  }
  if (levelIter != levelFiles.end()) {
    lastFileWasSelected = false;
    if ((upperBound.size() == 0 ||
         ucmp_->Compare(upperBound, (*levelIter)->smallest.user_key()) > 0)) {
      upperBound = (*levelIter)->smallest.user_key();
    }
  }
}

// get two ranges
// (smallExcluded, largeExcluded) all the keys in the selected files should be
// in the middle
// [smallestKey, largestKey] the slected file should contains keys
// in the range
void HybridCompactionPicker::expandSelection(
    const std::vector<FileMetaData*>& levelFiles,
    std::vector<FileMetaData*>& outFiles, Slice& lowerBound, Slice& upperBound,
    const Slice& smallest, const Slice& largest, bool& lastFileWasSelected) {
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

  // Skip files if prev's last user key is the same as f's first user key
  if (f != levelFiles.begin()) {
    for (auto prevf = std::prev(f); f != levelFiles.end(); ++f, ++prevf) {
      if (ucmp_->Compare((*prevf)->largest.user_key(),
                         (*f)->smallest.user_key()) != 0) {
        break;
      }

      if (upperBound.size() != 0 &&
          ucmp_->Compare((*f)->smallest.user_key(), upperBound) >= 0) {
        break;
      }
    }
  }

  // setup lower bound if needed
  if (f != levelFiles.begin()) {
    auto prevf = std::prev(f);
    if (lowerBound.size() == 0 ||
        ucmp_->Compare((*prevf)->largest.user_key(), lowerBound) > 0) {
      lowerBound = (*prevf)->largest.user_key();
    }
  }

  // we are at the spot take all the files in the range smallest largest that
  // have largestKey < upperbound
  if (f != levelFiles.end()) {
    for (; f != levelFiles.end(); f++) {
      if ((largest.size() != 0 &&
           ucmp_->Compare((*f)->smallest.user_key(), largest) > 0) ||
          (upperBound.size() != 0 &&
           ucmp_->Compare((*f)->largest.user_key(), upperBound) >= 0)) {
        break;
      }
      // file is contained
      outFiles.push_back(*f);
    }

    if (f != levelFiles.end()) {
      // Pop up files if the next files countains the same user key f is
      // pointing to next file
      for (; !outFiles.empty(); --f) {
        auto curfile = outFiles.back();
        if (ucmp_->Compare((*f)->smallest.user_key(),
                           curfile->largest.user_key()) != 0) {
          break;
        }
        outFiles.pop_back();
      }

      // setup the upper bound if needed
      if (upperBound.size() == 0 ||
          ucmp_->Compare((*f)->smallest.user_key(), upperBound) < 0) {
        upperBound = (*f)->smallest.user_key();
      }
      if (upperBound.size() == 0 ||
          ucmp_->Compare((*f)->largest.user_key(), upperBound) > 0) {
        lastFileWasSelected = false;
      }
    }
  }
}

bool HybridCompactionPicker::SelectNBuffers(
    std::vector<CompactionInputFiles>& inputs, size_t nBuffers,
    size_t outputLevel, size_t hyperLevelNum, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  const size_t lowest_level = LastLevelInHyper(hyperLevelNum);
  if (vstorage->LevelFiles(int(lowest_level)).empty()) {
    return false;
  }

  size_t upper_level = FirstLevelInHyper(hyperLevelNum) + 3;
  if (!prevSubCompaction_[hyperLevelNum - 1].empty() &&
      upper_level <= prevSubCompaction_[hyperLevelNum - 1].outputLevel) {
    upper_level = prevSubCompaction_[hyperLevelNum - 1].outputLevel + 1;
    if (upper_level > lowest_level) {
      return false;
    }
  }

  assert(lowest_level >= upper_level);
  size_t count = 0;
  for (size_t s = lowest_level; s >= upper_level; s--) {
    auto& levelFiles = vstorage->LevelFiles(int(s));
    if (!levelFiles.empty()) {
      count++;
    }
  }

  // select buffers from start level
  inputs.resize(count + 1);
  count--;

  Slice lowerBound, upperBound;
  Slice smallestKey, largestKey;

  bool lastFileWasSelected = true;
  inputs[count].level = int(lowest_level);
  selectNBufferFromFirstLevel(vstorage->LevelFiles(int(lowest_level)),
                              vstorage->LevelFiles(int(LastLevel())), nBuffers,
                              inputs[count].files, smallestKey, largestKey,
                              lowerBound, upperBound, lastFileWasSelected);
  const Slice prevPlace(prevSubCompaction_[hyperLevelNum].lastKey);
  if (!prevPlace.empty()) {
    if (ucmp_->Compare(prevPlace, smallestKey) < 0 &&
        (lowerBound.empty() || ucmp_->Compare(prevPlace, lowerBound) > 0)) {
      lowerBound = prevPlace;
    }
  }
  if (enable_spdb_log) {
    ROCKS_LOG_BUFFER(
        log_buffer, " Hybrid: select files for level %lu, (%s [%s %s] %s)",
        lowest_level,
        lowerBound.empty() ? "NULL" : lowerBound.ToString(true).c_str(),
        smallestKey.empty() ? "NULL" : smallestKey.ToString(true).c_str(),
        largestKey.empty() ? "NULL" : largestKey.ToString(true).c_str(),
        upperBound.empty() ? "NULL" : upperBound.ToString(true).c_str());
  }

  for (size_t level = lowest_level - 1; level >= upper_level; level--) {
    if (!vstorage->LevelFiles(int(level)).empty()) {
      count--;
      inputs[count].level = int(level);
      expandSelection(vstorage->LevelFiles(int(level)), inputs[count].files,
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
      if (enable_spdb_log) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            " Hybrid: expand selection for level %lu, (%s [%s %s] %s)", level,
            lowerBound.empty() ? "NULL" : lowerBound.ToString(true).c_str(),
            smallestKey.empty() ? "NULL" : smallestKey.ToString(true).c_str(),
            largestKey.empty() ? "NULL" : largestKey.ToString(true).c_str(),
            upperBound.empty() ? "NULL" : upperBound.ToString(true).c_str());
      }
    }
  }
  assert(count == 0);
  count = inputs.size() - 1;
  inputs[count].level = int(outputLevel);
  auto& fl = vstorage->LevelFiles(int(outputLevel));
  auto iter = locateFile(fl, smallestKey, fl.begin());
  // SPDB-228: if the smallest of the file is the same as the largest of the
  // prev add the prev file as well....
  if (iter != fl.end()) {
    while (iter != fl.begin()) {
      auto prev = std::prev(iter);
      if (ucmp_->Compare((*iter)->smallest.user_key(),
                         (*prev)->largest.user_key()) == 0) {
        iter = prev;
      } else {
        break;
      }
    }
  }

  auto& target_fl = inputs[count].files;
  for (; iter != fl.end(); iter++) {
    if (ucmp_->Compare((*iter)->smallest.user_key(), largestKey) > 0) {
      // SPDB-228: take additional files if needed to ensure the compaction
      // select all the versions of the same user key
      if (target_fl.empty() ||
          ucmp_->Compare((*iter)->smallest.user_key(),
                         target_fl.back()->largest.user_key()) > 0) {
        if (enable_spdb_log) {
          ROCKS_LOG_BUFFER(log_buffer,
                           " Hybrid: finish for outputLevel %lu, stopped at %s "
                           " largest is %s",
                           outputLevel,
                           (*iter)->smallest.user_key().ToString(true).c_str(),
                           largestKey.ToString(true).c_str());
        }
        break;
      }
    }
    inputs[count].files.push_back(*iter);
  }
  // trivial move ?
  // one level with data at count -1

  if (inputs[count].empty()) {
    bool trivial_move = true;
    for (size_t inp = 0; inp + 2 < count; inp++) {
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
    prevSubCompaction_[hyperLevelNum].lastKey.assign(upperBound.data(),
                                                     upperBound.size());
  } else {
    prevSubCompaction_[hyperLevelNum].lastKey.clear();
  }
  return true;
}

void HybridCompactionPicker::PrintLsmState(EventLoggerStream& stream,
                                           const VersionStorageInfo* vstorage) {
  if (enable_spdb_log) {
    CompactionPicker::PrintLsmState(stream, vstorage);
  }

  stream << "level_size";
  stream.StartArray();
  for (size_t level = 0; level <= curNumOfHyperLevels_; ++level) {
    stream << CalculateHyperlevelSize(level, vstorage) / 1024 / 1024;
  }
  stream << vstorage->NumLevelBytes(int(LastLevel())) / 1024 / 1024;

  stream.EndArray();
}

}  // namespace ROCKSDB_NAMESPACE
