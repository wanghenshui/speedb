#pragma once

#include "table/block_based/index_builder.h"
#include "table/block_based/spdb_index/spdb_segment_index_builder.h"

namespace ROCKSDB_NAMESPACE {

/*
 * SpdbTwoLevelndexBuilder is SPDB's index and filter builder.
 * TODO - Add documentation
 */
class SpdbTwoLevelndexBuilder : public IndexBuilder {
 public:
  static SpdbTwoLevelndexBuilder* CreateIndexBuilder(
      const InternalKeyComparator* comparator,
      const bool use_value_delta_encoding,
      const BlockBasedTableOptions& table_opt);

  void AddIndexEntry(std::string* last_key_in_current_block,
                     const Slice* first_key_in_next_block,
                     const BlockHandle& block_handle) override;

  // The approach here is the same as the one used in PartitionedIndexBuilder;
  // Finish will be called repeatedly, once for every segment, and once for the
  // top-level.
  // It returns Status::Incomplete() for the partitions, and Status::OK() for
  // the top-level (last call) in which case the caller will not call it again
  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& prev_segment_block_handle) override;

  size_t IndexSize() const override { return index_size_; }

  size_t TopLevelIndexSize(uint64_t) const { return top_level_index_size_; }

  size_t NumSegments() const { return segments_.size(); }

  bool seperator_is_key_plus_seq() override {
    return seperator_is_key_plus_seq_;
  }
  bool get_use_value_delta_encoding() { return use_value_delta_encoding_; }

 private:
  using SegmentPtr = std::unique_ptr<SpdbSegmentIndexBuilder>;

 private:
  SpdbTwoLevelndexBuilder(const InternalKeyComparator* comparator,
                          const BlockBasedTableOptions& table_opt,
                          const bool use_value_delta_encoding);

  void MakeNewSegmentIndexBuilderIfNecessary();
  void FinalizeCurrSegmentIfApplicable(bool force_finalization);

  void AddNextSegmentToTopLevelIndex(
      const BlockHandle& last_segment_block_handle,
      const SpdbSegmentIndexBuilder* next_segment);

 private:
  // Set after ::Finish is called
  size_t top_level_index_size_ = 0;

  BlockBuilder top_level_block_builder_;              // top-level index builder
  BlockBuilder top_level_block_builder_without_seq_;  // same for user keys

  std::list<SegmentPtr> segments_;  // list of segmented indexes and their keys

  // the active segment index builder
  SegmentPtr curr_segment_;

  // true if Finish was called at leat once but not complete yet (expecting
  // additional calls)
  bool first_call_to_finish_ = true;

  const BlockBasedTableOptions& table_opt_;
  bool seperator_is_key_plus_seq_;
  bool use_value_delta_encoding_;
  BlockHandle last_encoded_handle_;
};

}  // namespace ROCKSDB_NAMESPACE