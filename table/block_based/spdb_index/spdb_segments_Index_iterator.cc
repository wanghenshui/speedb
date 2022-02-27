#include "table/block_based/spdb_index/spdb_segments_Index_iterator.h"

namespace ROCKSDB_NAMESPACE {
void SpdbSegmentsIndexIterator::Seek(const Slice& target) { SeekImpl(&target); }

void SpdbSegmentsIndexIterator::SeekToFirst() { SeekImpl(nullptr); }

void SpdbSegmentsIndexIterator::SeekImpl(const Slice* target) {
  SavePrevIndexValue();

  if (target) {
    index_iter_->Seek(*target);
  } else {
    index_iter_->SeekToFirst();
  }

  if (!index_iter_->Valid()) {
    ResetSegmentsIndexIterator();
    return;
  }

  InitSegmentIndexBlock();

  if (target) {
    segment_iter_.Seek(*target);
  } else {
    segment_iter_.SeekToFirst();
  }
  FindKeyForward();

  // We could check upper bound here, but that would be too complicated
  // and checking index upper bound is less useful than for data blocks.

  if (target) {
    assert(!Valid() || (table_->get_rep()->index_key_includes_seq
                            ? (icomp_.Compare(*target, key()) <= 0)
                            : (user_comparator_.Compare(ExtractUserKey(*target),
                                                        key()) <= 0)));
  }
}

void SpdbSegmentsIndexIterator::SeekToLast() {
  SavePrevIndexValue();
  index_iter_->SeekToLast();
  if (!index_iter_->Valid()) {
    ResetSegmentsIndexIterator();
    return;
  }
  InitSegmentIndexBlock();
  segment_iter_.SeekToLast();
  FindKeyBackward();
}

void SpdbSegmentsIndexIterator::Next() {
  assert(segment_iter_points_to_real_block_);
  segment_iter_.Next();
  FindKeyForward();
}

void SpdbSegmentsIndexIterator::Prev() {
  assert(segment_iter_points_to_real_block_);
  segment_iter_.Prev();

  FindKeyBackward();
}

void SpdbSegmentsIndexIterator::InitSegmentIndexBlock() {
  BlockHandle segment_handle = index_iter_->value().handle;

  if (!segment_iter_points_to_real_block_ ||
      segment_handle.offset() != prev_segment_offset_ ||
      // if previous attempt of reading the block missed cache, try again
      segment_iter_.status().IsIncomplete()) {
    if (segment_iter_points_to_real_block_) {
      ResetSegmentsIndexIterator();
    }
    auto* rep = table_->get_rep();
    bool is_for_compaction =
        lookup_context_.caller == TableReaderCaller::kCompaction;
    // Prefetch additional data for range scans (iterators).
    // Implicit auto readahead:
    //   Enabled after 2 sequential IOs when ReadOptions.readahead_size == 0.
    // Explicit user requested readahead:
    //   Enabled from the very first IO when ReadOptions.readahead_size is set.
    block_prefetcher_.PrefetchIfNeeded(
        rep, segment_handle, read_options_.readahead_size, is_for_compaction);

    Status s;
    table_->NewDataBlockIterator<IndexBlockIter>(
        read_options_, segment_handle, &segment_iter_, BlockType::kIndex,
        /*get_context=*/nullptr, &lookup_context_, s,
        block_prefetcher_.prefetch_buffer(),
        /*for_compaction=*/is_for_compaction);
    segment_iter_points_to_real_block_ = true;
    // We could check upper bound here but it is complicated to reason about
    // upper bound in index iterator. On the other than, in large scans, index
    // iterators are moved much less frequently compared to data blocks. So
    // the upper bound check is skipped for simplicity.
  }
}

void SpdbSegmentsIndexIterator::FindKeyForward() {
  // This method's code is kept short to make it likely to be inlined.

  assert(segment_iter_points_to_real_block_);

  if (!segment_iter_.Valid()) {
    // This is the only call site of FindBlockForward(), but it's extracted into
    // a separate method to keep FindKeyForward() short and likely to be
    // inlined. When transitioning to a different block, we call
    // FindBlockForward(), which is much longer and is probably not inlined.
    FindBlockForward();
  } else {
    // This is the fast path that avoids a function call.
  }
}

void SpdbSegmentsIndexIterator::FindBlockForward() {
  // TODO the while loop inherits from two-level-iterator. We don't know
  // whether a block can be empty so it can be replaced by an "if".
  do {
    if (!segment_iter_.status().ok()) {
      return;
    }
    ResetSegmentsIndexIterator();
    index_iter_->Next();

    if (!index_iter_->Valid()) {
      return;
    }

    InitSegmentIndexBlock();
    segment_iter_.SeekToFirst();
  } while (!segment_iter_.Valid());
}

void SpdbSegmentsIndexIterator::FindKeyBackward() {
  while (!segment_iter_.Valid()) {
    if (!segment_iter_.status().ok()) {
      return;
    }

    ResetSegmentsIndexIterator();
    index_iter_->Prev();

    if (index_iter_->Valid()) {
      InitSegmentIndexBlock();
      segment_iter_.SeekToLast();
    } else {
      return;
    }
  }
}

void SpdbSegmentsIndexIterator::SeekForPrev(const Slice&) {
  // Shouldn't be called.
  assert(false);
}

bool SpdbSegmentsIndexIterator::NextAndGetResult(IterateResult*) {
  assert(false);
  return false;
}

inline IterBoundCheck SpdbSegmentsIndexIterator::UpperBoundCheckResult() {
  // Shouldn't be called.
  assert(false);
  return IterBoundCheck::kUnknown;
}
void SpdbSegmentsIndexIterator::SetPinnedItersMgr(PinnedIteratorsManager*) {
  // Shouldn't be called.
  assert(false);
}

bool SpdbSegmentsIndexIterator::IsKeyPinned() const {
  // Shouldn't be called.
  assert(false);
  return false;
}

bool SpdbSegmentsIndexIterator::IsValuePinned() const {
  // Shouldn't be called.
  assert(false);
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
