#pragma once

#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

class SpdbSegmentIndexBuilder : public IndexBuilder {
 public:
  SpdbSegmentIndexBuilder(const InternalKeyComparator* comparator,
                          const BlockBasedTableOptions& table_opt,
                          const bool use_value_delta_encoding,
                          bool seperator_is_key_plus_seq);

  void AddIndexEntry(std::string* last_key_in_current_block,
                     const Slice* first_key_in_next_block,
                     const BlockHandle& block_handle) override;

  Status Finish(IndexBlocks* index_blocks,
                const BlockHandle& last_partition_block_handle) override;

  size_t IndexSize() const override { return index_size_; }

  bool seperator_is_key_plus_seq() override {
    return equidistant_index_builder_->seperator_is_key_plus_seq_;
  }

 public:
  void SetSeperatorIsKeyPlusSeq(bool seperator_is_key_plus_seq) {
    equidistant_index_builder_->seperator_is_key_plus_seq_ =
        seperator_is_key_plus_seq;
  }

  bool CanAcceptAdditionalKeys() const {
    return (num_blocks_in_curr_segment_ < 256);
  }

  Slice GetLastKeyOfLastAddedBlock() const {
    return Slice(last_key_of_last_added_block_);
  }

 private:
  void MakeEquidistantIndexBuilder(const BlockBasedTableOptions& table_opt,
                                   bool use_value_delta_encoding);

 private:
  size_t num_blocks_in_curr_segment_ = 0U;
  std::unique_ptr<ShortenedIndexBuilder> equidistant_index_builder_;

  // the last key in the active partition index builder
  std::string last_key_of_last_added_block_;

  bool seperator_is_key_plus_seq_;
};

}  // namespace ROCKSDB_NAMESPACE