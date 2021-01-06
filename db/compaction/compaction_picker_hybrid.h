//  Copyright (c) 2011-present, YrocksDb, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// The file contains the class CompactionPickerHybrid, which is an
// implementation of the abstruct class CompactionPicker (rocksdb)

#pragma once

#include <mutex>

#include "db/compaction/compaction_picker.h"
#define UserKey Slice

namespace ROCKSDB_NAMESPACE {

// short descriptors of the running compactions
struct HybridCompactionDescriptor {
  uint nCompactions;
  uint startLevel;
  bool hasRearange;
};

class HybridComactionsDescribtors
    : public std::vector<HybridCompactionDescriptor> {
 public:
  HybridComactionsDescribtors(size_t size)
      : std::vector<HybridCompactionDescriptor>(size) {}
  bool rearangeRunning;
};

static const size_t s_maxFilesToCompact = 60;
static const size_t s_minLevelsToMerge = 4;
static const size_t s_maxLevelsToMerge = 8;
static const size_t s_maxNumHyperLevels = 10;
static const size_t s_minNumHyperLevels = 1;
static const size_t s_levelsInHyperLevel = (s_maxLevelsToMerge + 4) * 2;

class HybridCompactionPicker : public CompactionPicker {
 public:
  HybridCompactionPicker(const ImmutableOptions& ioptions,
                         const InternalKeyComparator* icmp);

  ~HybridCompactionPicker() override{};

 public:
  // pick a compaction
  virtual Compaction* PickCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer,
      SequenceNumber earliest_memtable_seqno = kMaxSequenceNumber) override;

  // check if the cf needs compaction
  bool NeedsCompaction(const VersionStorageInfo* vstorage) const override;

  // set and optimize the cf options to work with hybrid compaction
  static void SetOptions(ColumnFamilyOptions& options) {
    options.compaction_style = kCompactionStyleHybrid;
    // one for L0 and one for L(last in case of max db)
    // num level was set

    if ((uint)options.num_levels <
        (s_minNumHyperLevels * s_levelsInHyperLevel) + 2) {
      size_t requiredLevel =
          std::max(s_minNumHyperLevels,
                   std::min((size_t)options.num_levels, s_maxNumHyperLevels));
      options.num_levels = (requiredLevel * s_levelsInHyperLevel) + 2;
    }
    if (options.compaction_options_universal.min_merge_width < 4 ||
        options.compaction_options_universal.min_merge_width >
            (int)s_maxLevelsToMerge) {
      options.compaction_options_universal.min_merge_width = s_maxLevelsToMerge;
    }

    uint& space_amp =
        options.compaction_options_universal.max_size_amplification_percent;
    if (space_amp > 200) {
      space_amp = 200;
    } else if (space_amp < 110) {
      space_amp = 110;
    }
    options.OptimizeForPointLookup(0);
  }

 private:
  // build a descriptor of all the running compactions.
  void BuildCompactionDescriptors(HybridComactionsDescribtors& out) const;

  // reatange: move level to the highest level in the hyper that is empty
  Compaction* RearangeLevel(uint HyperlevelNum, const std::string& cf_name,
                            const MutableCFOptions& mutable_cf_options,
                            const MutableDBOptions& mutable_db_options,
                            VersionStorageInfo* vstorage);

  // level 0 compaction is merging Level 0 files to the highest level that is
  // free in hyper level 1
  Compaction* PickLevel0Compaction(LogBuffer* log_buffer,
                                   const std::string& cf_name,
                                   const MutableCFOptions& mutable_cf_options,
                                   const MutableDBOptions& mutable_db_options,
                                   VersionStorageInfo* vstorage,
                                   size_t mergeWidth);

  // other level compaction pick few levels and merge them to the
  // highest level that is free in next hype level
  Compaction* PickLevelCompaction(LogBuffer* log_buffer, uint HyperlevelNum,
                                  const std::string& cf_name,
                                  const MutableCFOptions& mutable_cf_options,
                                  const MutableDBOptions& mutable_db_options,
                                  VersionStorageInfo* vstorage,
                                  bool lowPriority = false);

  // checking the database size (creating a new hyper level if the size is too
  // large)
  Compaction* CheckDbSize(const std::string& cf_name,
                          const MutableCFOptions& mutable_cf_options,
                          const MutableDBOptions& mutable_db_options,
                          VersionStorageInfo* vstorage, LogBuffer* log_buffer);

  Compaction* MoveSstToLastLevel(const std::string& cf_name,
                                 const MutableCFOptions& mutable_cf_options,
                                 const MutableDBOptions& mutable_db_options,
                                 VersionStorageInfo* vstorage,
                                 LogBuffer* log_buffer);

  Compaction* PickReduceNumLevels(LogBuffer* log_buffer, uint hyperLevelNum,
                                  const std::string& cfName,
                                  const MutableCFOptions& mutable_cf_options,
                                  const MutableDBOptions& mutable_db_options,
                                  VersionStorageInfo* vstorage);

 private:
  static uint LastLevelThreadsNum(uint spaceAmp) {
    // for debug purpose
    // return 1;
    if (spaceAmp >= 200) {
      return 2;
    } else if (spaceAmp <= 110) {
      return 10;
    } else {
      return (100 / (spaceAmp - 100));
    }
  }

  static uint FirstLevelInHyper(uint hyperLevelNum) {
    if (hyperLevelNum == 0) {
      return 0;
    } else {
      return (hyperLevelNum - 1) * s_levelsInHyperLevel + 1;
    }
  }

  static uint LastLevelInHyper(uint hyperLevelNum) {
    if (hyperLevelNum == 0) {
      return 0;
    } else {
      return s_levelsInHyperLevel * hyperLevelNum;
    }
  }

  static uint GetHyperLevelNum(uint level) {
    if (level == 0) {
      return 0;
    }
    return ((level - 1) / s_levelsInHyperLevel) + 1;
  }

  uint LastLevel() const { return LastLevelInHyper(curNumOfHyperLevels_) + 1; }

  bool LevelNeedsRearange(uint hyperLevelNum,
                          const VersionStorageInfo* vstorage) const;
  static size_t CalculateHyperlevelSize(uint hyperLevelNum,
                                        const VersionStorageInfo* vstorage);

  void InitCf(const MutableCFOptions& mutable_cf_options,
              VersionStorageInfo* vstorage);

  bool MayRunRearange(
      uint hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel) const;

  bool MayRunCompaction(
      uint hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel) const;

  bool MayStartLevelCompaction(
      uint hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel,
      const VersionStorageInfo* vstorage) const;

  bool NeedToRunLevelCompaction(uint hyperLevelNum,
                                const VersionStorageInfo* vstorage) const;

  bool SelectNBuffers(std::vector<CompactionInputFiles>& inputs, uint nBuffers,
                      uint outputLevel, uint hyperLevelNum,
                      VersionStorageInfo* vstorage, const std::string& cf_name,
                      LogBuffer* log_buffer);

  void expandSelection(const std::vector<FileMetaData*>& levelFiles,
                       std::vector<FileMetaData*>& outFiles,
                       UserKey& smallestExcludedKey,
                       UserKey& largestExcludedKey, const UserKey& smallest,
                       const UserKey& largest, const std::string& cf_name,
                       LogBuffer* log_buffer);

  void selectNBufferFromFirstLevel(
      const std::vector<FileMetaData*>& levelFiles,
      const std::vector<FileMetaData*>& targetLevelFiles, uint maxNBuffers,
      std::vector<FileMetaData*>& outFiles, UserKey& smallestKey,
      UserKey& largestKey, UserKey& smallestExcludedKey,
      UserKey& largestExcludedKey);

  std::vector<FileMetaData*>::const_iterator locateFileLarger(
      const std::vector<FileMetaData*>& filesList, const UserKey& key);

  std::vector<FileMetaData*>::const_iterator locateFile(
      const std::vector<FileMetaData*>& filesList, const UserKey& key,
      const std::vector<FileMetaData*>::const_iterator& start);

  void EnableLowPriorityCompaction(bool enable) override {
    enableLow_ = enable;
  };

  void PrintLsmState(EventLoggerStream& stream,
                     const VersionStorageInfo* vstorage) override;

 private:
  // not sure if needed but this prevent running twice from two different
  // contents
  // std::Mutex mutex_;

  // uint64_t   dbSize_;
  // those parameters are re-calculate each time the database increase it size
  // above the current db_size
  uint curNumOfHyperLevels_;
  uint maxNumHyperLevels_;
  uint lastLevelThreads_;
  size_t sizeToCompact_[s_maxNumHyperLevels + 1];
  size_t multiplier_[s_maxNumHyperLevels + 1];
  size_t lastLevelSizeCompactionStart_;
  bool enableLow_;
  double spaceAmpFactor_;
  const Comparator* ucmp_;
  struct PrevPlace {
    PrevPlace() : outputLevel(-1u) {}
    bool empty() const { return outputLevel == -1u; }
    void setEmpty() { outputLevel = -1u; }
    uint outputLevel;
    UserKey lastKey;
  };
  PrevPlace prevSubCompaction_[s_maxNumHyperLevels];
};
}  // namespace ROCKSDB_NAMESPACE
