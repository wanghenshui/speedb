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
#include <string>

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

// short descriptors of the running compactions
struct HybridCompactionDescriptor {
  size_t nCompactions;
  size_t startLevel;
  bool hasRearange;
};

class HybridComactionsDescribtors
    : public std::vector<HybridCompactionDescriptor> {
 public:
  HybridComactionsDescribtors(size_t size)
      : std::vector<HybridCompactionDescriptor>(size) {}
  bool rearangeRunning;
  bool manualComapctionRunning;
};

class HybridCompactionPicker : public CompactionPicker {
  static constexpr size_t kFilesToCompactMax = 60;
  static constexpr size_t kLevelsToMergeMin = 4;
  static constexpr size_t kLevelsToMergeMax = 8;
  static constexpr size_t kHyperLevelsNumMax = 10;
  static constexpr size_t kHyperLevelsNumMin = 1;
  static constexpr size_t kLevelsInHyperLevel = (kLevelsToMergeMax + 4) * 2;

 public:
  HybridCompactionPicker(const ImmutableOptions& ioptions,
                         const InternalKeyComparator* icmp);

  ~HybridCompactionPicker() override = default;

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

    const int required_mult =
        (options.compaction_options_universal.min_merge_width <
             kLevelsToMergeMin ||
         options.compaction_options_universal.min_merge_width >
             kLevelsToMergeMax)
            ? kLevelsToMergeMax
            : options.compaction_options_universal.min_merge_width;

    if (options.level0_file_num_compaction_trigger >= 0 &&
        options.level0_file_num_compaction_trigger < required_mult &&
        options.level0_slowdown_writes_trigger > required_mult &&
        options.level0_stop_writes_trigger >=
            options.level0_slowdown_writes_trigger) {
      options.level0_file_num_compaction_trigger = required_mult;
    }

    // one for L0 and one for L(last in case of max db)
    // num level was set
    if (size_t(options.num_levels) <
        (kHyperLevelsNumMin * kLevelsInHyperLevel) + 2) {
      size_t requiredLevel =
          std::max(kHyperLevelsNumMin,
                   std::min<size_t>(options.num_levels, kHyperLevelsNumMax));
      options.num_levels =
          static_cast<int>((requiredLevel * kLevelsInHyperLevel) + 2);
    }
    if (options.compaction_options_universal.min_merge_width < 4 ||
        options.compaction_options_universal.min_merge_width >
            (int)kLevelsToMergeMax) {
      options.compaction_options_universal.min_merge_width = kLevelsToMergeMax;
    }
    uint& space_amp =
        options.compaction_options_universal.max_size_amplification_percent;
    if (space_amp > 200) {
      space_amp = 200;
    } else if (space_amp < 110) {
      space_amp = 110;
    }

    if (!options.comparator->CanKeysWithDifferentByteContentsBeEqual()) {
      options.memtable_whole_key_filtering = true;
    }
  }

 private:
  // build a descriptor of all the running compactions.
  void BuildCompactionDescriptors(HybridComactionsDescribtors& out) const;

  // reatange: move level to the highest level in the hyper that is empty
  Compaction* RearangeLevel(size_t HyperlevelNum, const std::string& cf_name,
                            const MutableCFOptions& mutable_cf_options,
                            const MutableDBOptions& mutable_db_options,
                            VersionStorageInfo* vstorage);

  // level 0 compaction is merging Level 0 files to the highest level that is
  // free in hyper level 1
  Compaction* PickLevel0Compaction(const MutableCFOptions& mutable_cf_options,
                                   const MutableDBOptions& mutable_db_options,
                                   VersionStorageInfo* vstorage,
                                   size_t mergeWidth);

  // other level compaction pick few levels and merge them to the
  // highest level that is free in next hype level
  Compaction* PickLevelCompaction(size_t HyperlevelNum,
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

  Compaction* PickReduceNumFiles(const MutableCFOptions& mutable_cf_options,
                                 const MutableDBOptions& mutable_db_options,
                                 VersionStorageInfo* vstorage,
                                 size_t minFileSize);

 private:
  static size_t FirstLevelInHyper(size_t hyperLevelNum) {
    if (hyperLevelNum == 0) {
      return 0;
    } else {
      return (hyperLevelNum - 1) * kLevelsInHyperLevel + 1;
    }
  }

  static size_t LastLevelInHyper(size_t hyperLevelNum) {
    if (hyperLevelNum == 0) {
      return 0;
    } else {
      return kLevelsInHyperLevel * hyperLevelNum;
    }
  }

  static size_t GetHyperLevelNum(size_t level) {
    if (level == 0) {
      return 0;
    }
    return ((level - 1) / kLevelsInHyperLevel) + 1;
  }

  size_t LastLevel() const {
    return LastLevelInHyper(curNumOfHyperLevels_) + 1;
  }

  bool LevelNeedsRearange(size_t hyperLevelNum,
                          const VersionStorageInfo* vstorage,
                          size_t firstLevel) const;

  static size_t CalculateHyperlevelSize(size_t hyperLevelNum,
                                        const VersionStorageInfo* vstorage);

  void InitCf(const MutableCFOptions& mutable_cf_options,
              VersionStorageInfo* vstorage);

  bool MayRunRearange(
      size_t hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel) const;

  bool MayRunCompaction(
      size_t hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel) const;

  bool MayStartLevelCompaction(
      size_t hyperLevelNum,
      const HybridComactionsDescribtors& compactionJobsPerLevel,
      const VersionStorageInfo* vstorage) const;

  bool NeedToRunLevelCompaction(size_t hyperLevelNum,
                                const VersionStorageInfo* vstorage) const;

  bool SelectNBuffers(std::vector<CompactionInputFiles>& inputs,
                      size_t nBuffers, size_t outputLevel, size_t hyperLevelNum,
                      VersionStorageInfo* vstorage);

  void expandSelection(const std::vector<FileMetaData*>& levelFiles,
                       std::vector<FileMetaData*>& outFiles,
                       Slice& smallestExcludedKey, Slice& largestExcludedKey,
                       const Slice& smallest, const Slice& largest,
                       bool& lastFileWasSelected);

  void selectNBufferFromFirstLevel(
      const std::vector<FileMetaData*>& levelFiles,
      const std::vector<FileMetaData*>& targetLevelFiles, size_t maxNBuffers,
      std::vector<FileMetaData*>& outFiles, Slice& smallestKey,
      Slice& largestKey, Slice& smallestExcludedKey, Slice& largestExcludedKey,
      bool& lastFileWasSelected);

  std::vector<FileMetaData*>::const_iterator locateFile(
      const std::vector<FileMetaData*>& filesList, const Slice& key,
      const std::vector<FileMetaData*>::const_iterator& start) const;

  bool Intersecting(const std::vector<FileMetaData*>& f1,
                    const std::vector<FileMetaData*>& f2) const;

  bool Intersecting(const FileMetaData* f1,
                    const std::vector<FileMetaData*>& f2) const;

  void EnableLowPriorityCompaction(bool enable) override {
    enableLow_ = enable;
  };

  void PrintLsmState(EventLoggerStream& stream,
                     const VersionStorageInfo* vstorage) override;

 private:
  // not sure if needed but this prevent running twice from two different
  // contents
  port::Mutex mutex_;

  // uint64_t   dbSize_;
  // those parameters are re-calculate each time the database increase it size
  // above the current db_size
  size_t curNumOfHyperLevels_;
  size_t maxNumHyperLevels_;
  size_t sizeToCompact_[kHyperLevelsNumMax + 1];
  size_t multiplier_[kHyperLevelsNumMax + 1];
  size_t lastLevelSizeCompactionStart_;
  size_t level0_compaction_trigger_;
  bool enableLow_;
  double spaceAmpFactor_;
  const Comparator* ucmp_;
  struct PrevPlace {
    PrevPlace() : outputLevel(-1u) {}
    bool empty() const { return outputLevel == -1u; }
    void setEmpty() { outputLevel = -1u; }
    size_t outputLevel;
    std::string lastKey;
  };
  PrevPlace prevSubCompaction_[kHyperLevelsNumMax];
  size_t max_open_files_;
};

}  // namespace ROCKSDB_NAMESPACE
