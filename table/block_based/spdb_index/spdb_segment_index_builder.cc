#include "table/block_based/spdb_index/spdb_segment_index_builder.h"

namespace ROCKSDB_NAMESPACE {

SpdbSegmentIndexBuilder::SpdbSegmentIndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding, bool seperator_is_key_plus_seq)
    : IndexBuilder(comparator),
      seperator_is_key_plus_seq_(seperator_is_key_plus_seq) {
  MakeEquidistantIndexBuilder(table_opt, use_value_delta_encoding);
}

void SpdbSegmentIndexBuilder::MakeEquidistantIndexBuilder(
    const BlockBasedTableOptions& table_opt, bool use_value_delta_encoding) {
  equidistant_index_builder_.reset(new ShortenedIndexBuilder(
      comparator_, table_opt.index_block_restart_interval,
      table_opt.format_version, use_value_delta_encoding,
      table_opt.index_shortening,
      /* include_first_key */ false));

  // Set equidistant_index_builder_->seperator_is_key_plus_seq_ to true if
  // seperator_is_key_plus_seq_ is true (internal-key mode) (set to false by
  // default on Creation) so that flush policy can point to
  // equidistant_index_builder_->index_block_builder_
  if (seperator_is_key_plus_seq_) {
    equidistant_index_builder_->seperator_is_key_plus_seq_ = true;
  }

  num_blocks_in_curr_segment_ = 0u;
}

void SpdbSegmentIndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  assert(CanAcceptAdditionalKeys());
  if (CanAcceptAdditionalKeys() == false) {
    return;
  }

  equidistant_index_builder_->AddIndexEntry(
      last_key_in_current_block, first_key_in_next_block, block_handle);
  ++num_blocks_in_curr_segment_;

  last_key_of_last_added_block_ = std::string(*last_key_in_current_block);
}

Status SpdbSegmentIndexBuilder::Finish(
    IndexBlocks* index_blocks,
    const BlockHandle& /* last_partition_block_handle*/) {
  return equidistant_index_builder_->Finish(index_blocks);
}

}  // namespace ROCKSDB_NAMESPACE