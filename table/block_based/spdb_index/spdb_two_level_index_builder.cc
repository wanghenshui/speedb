#include "table/block_based/spdb_index/spdb_two_level_index_builder.h"

namespace ROCKSDB_NAMESPACE {

SpdbTwoLevelndexBuilder* SpdbTwoLevelndexBuilder::CreateIndexBuilder(
    const InternalKeyComparator* comparator,
    const bool use_value_delta_encoding,
    const BlockBasedTableOptions& table_opt) {
  return new SpdbTwoLevelndexBuilder(comparator, table_opt,
                                     use_value_delta_encoding);
}

SpdbTwoLevelndexBuilder::SpdbTwoLevelndexBuilder(
    const InternalKeyComparator* comparator,
    const BlockBasedTableOptions& table_opt,
    const bool use_value_delta_encoding)
    : IndexBuilder(comparator),
      top_level_block_builder_(table_opt.index_block_restart_interval,
                               true /*use_delta_encoding*/,
                               use_value_delta_encoding),
      top_level_block_builder_without_seq_(
          table_opt.index_block_restart_interval, true /*use_delta_encoding*/,
          use_value_delta_encoding),
      table_opt_(table_opt),
      // We start by false. After each segment we revise the value based on
      // what the segment_builder has decided. If the feature is disabled
      // entirely, this will be set to true after switching the first
      // segment_builder. Otherwise, it could be set to true even one of the
      // segment_builders could not safely exclude seq from the keys, then it
      // wil be enforced on all segment_builders on ::Finish.
      seperator_is_key_plus_seq_(false),
      use_value_delta_encoding_(use_value_delta_encoding) {}

void SpdbTwoLevelndexBuilder::MakeNewSegmentIndexBuilderIfNecessary() {
  if (curr_segment_ == nullptr) {
    curr_segment_.reset(new SpdbSegmentIndexBuilder(
        comparator_, table_opt_, use_value_delta_encoding_,
        seperator_is_key_plus_seq_));
  }
}

void SpdbTwoLevelndexBuilder::FinalizeCurrSegmentIfApplicable(
    bool force_finalization) {
  assert(curr_segment_);

  if (force_finalization ||
      (curr_segment_->CanAcceptAdditionalKeys() == false)) {
    segments_.push_back(std::move(curr_segment_));
  }
}

void SpdbTwoLevelndexBuilder::AddIndexEntry(
    std::string* last_key_in_current_block,
    const Slice* first_key_in_next_block, const BlockHandle& block_handle) {
  MakeNewSegmentIndexBuilderIfNecessary();

  curr_segment_->AddIndexEntry(last_key_in_current_block,
                               first_key_in_next_block, block_handle);

  seperator_is_key_plus_seq_ |= curr_segment_->seperator_is_key_plus_seq();

  // Force finalization in case this is the last call => last segment
  auto force_segment_finalization = (first_key_in_next_block == nullptr);
  FinalizeCurrSegmentIfApplicable(force_segment_finalization);
}

void SpdbTwoLevelndexBuilder::AddNextSegmentToTopLevelIndex(
    const BlockHandle& last_segment_block_handle,
    const SpdbSegmentIndexBuilder* next_segment) {
  assert(last_segment_block_handle.IsValid());

  std::string handle_delta_encoding;
  PutVarsignedint64(&handle_delta_encoding, last_segment_block_handle.size() -
                                                last_encoded_handle_.size());
  last_encoded_handle_ = last_segment_block_handle;

  std::string handle_encoding;
  last_segment_block_handle.EncodeTo(&handle_encoding);
  const Slice handle_delta_encoding_slice(handle_delta_encoding);

  top_level_block_builder_.Add(next_segment->GetLastKeyOfLastAddedBlock(),
                               handle_encoding, &handle_delta_encoding_slice);
  if (!seperator_is_key_plus_seq_) {
    top_level_block_builder_without_seq_.Add(
        ExtractUserKey(next_segment->GetLastKeyOfLastAddedBlock()),
        handle_encoding, &handle_delta_encoding_slice);
  }
}

Status SpdbTwoLevelndexBuilder::Finish(
    IndexBlocks* index_blocks, const BlockHandle& prev_segment_block_handle) {
  // It must be set to null after last key is added
  assert(curr_segment_ == nullptr);

  if (first_call_to_finish_ == false) {
    AddNextSegmentToTopLevelIndex(prev_segment_block_handle,
                                  segments_.front().get());
    segments_.pop_front();
  } else {
    first_call_to_finish_ = false;
  }

  // If there is no segment left, then return the 2nd level index.
  if (UNLIKELY(segments_.empty())) {
    if (seperator_is_key_plus_seq_) {
      index_blocks->index_block_contents = top_level_block_builder_.Finish();
    } else {
      index_blocks->index_block_contents =
          top_level_block_builder_without_seq_.Finish();
    }

    top_level_index_size_ = index_blocks->index_block_contents.size();
    index_size_ += top_level_index_size_;

    return Status::OK();

  } else {
    // Finish the next segment index in line and Incomplete() to indicate we
    // expect more calls to Finish
    auto& next_segment = segments_.front();

    // Apply the policy to all sub-indexes
    next_segment->SetSeperatorIsKeyPlusSeq(seperator_is_key_plus_seq_);
    auto s = next_segment->Finish(index_blocks, prev_segment_block_handle);
    index_size_ += index_blocks->index_block_contents.size();

    return s.ok() ? Status::Incomplete() : s;
  }
}

}  // namespace ROCKSDB_NAMESPACE