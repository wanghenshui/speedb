#pragma once
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_based_table_reader_impl.h"
#include "table/block_based/block_prefetcher.h"
#include "table/block_based/reader_common.h"

namespace ROCKSDB_NAMESPACE {
// Iterator that iterates over SpeeDB's Segments index.
// Some upper and lower bound tricks played in block based table iterators
// could be played here, but it's too complicated to reason about index
// keys with upper or lower bound, so we skip it for simplicity.
class SpdbSegmentsIndexIterator : public InternalIteratorBase<IndexValue> {
 public:
  SpdbSegmentsIndexIterator(
      const BlockBasedTable* table, const ReadOptions& read_options,
      const InternalKeyComparator& icomp,
      std::unique_ptr<InternalIteratorBase<IndexValue>>&& index_iter,
      TableReaderCaller caller, size_t compaction_readahead_size = 0)
      : table_(table),
        read_options_(read_options),
#ifndef NDEBUG
        icomp_(icomp),
#endif
        user_comparator_(icomp.user_comparator()),
        index_iter_(std::move(index_iter)),
        segment_iter_points_to_real_block_(false),
        lookup_context_(caller),
        block_prefetcher_(compaction_readahead_size) {
  }

  ~SpdbSegmentsIndexIterator() override {}

 public:
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  void Prev() override;

 public:
  bool Valid() const override {
    return segment_iter_points_to_real_block_ && segment_iter_.Valid();
  }

  Slice key() const override {
    assert(Valid());
    return segment_iter_.key();
  }

  Slice user_key() const override {
    assert(Valid());
    return segment_iter_.user_key();
  }

  IndexValue value() const override {
    assert(Valid());
    return segment_iter_.value();
  }

  Status status() const override {
    // Prefix index set status to NotFound when the prefix does not exist
    if (!index_iter_->status().ok() && !index_iter_->status().IsNotFound()) {
      return index_iter_->status();
    } else if (segment_iter_points_to_real_block_) {
      return segment_iter_.status();
    } else {
      return Status::OK();
    }
  }

  void ResetSegmentsIndexIterator() {
    if (segment_iter_points_to_real_block_) {
      segment_iter_.Invalidate(Status::OK());
      segment_iter_points_to_real_block_ = false;
    }
  }

  void SavePrevIndexValue() {
    if (segment_iter_points_to_real_block_) {
      // Reseek. If they end up with the same data block, we shouldn't re-fetch
      // the same data block.
      prev_segment_offset_ = index_iter_->value().handle.offset();
    }
  }

 public:
  // ALL THESE MUST NOT BE CALLED AND ASSERT IF THEY ARE CALLED
  void SeekForPrev(const Slice&) override;
  bool NextAndGetResult(IterateResult*) override;
  IterBoundCheck UpperBoundCheckResult() override;
  void SetPinnedItersMgr(PinnedIteratorsManager*) override;
  bool IsKeyPinned() const override;
  bool IsValuePinned() const override;

 private:
  // If `target` is null, seek to first.
  void SeekImpl(const Slice* target);

  void InitSegmentIndexBlock();
  void FindKeyForward();
  void FindBlockForward();
  void FindKeyBackward();

 private:
  const BlockBasedTable* table_ = nullptr;
  const ReadOptions read_options_;

#ifndef NDEBUG
  const InternalKeyComparator& icomp_;
#endif
  UserComparatorWrapper user_comparator_;

  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter_;
  IndexBlockIter segment_iter_;

  // True if segment_iter_ is initialized and points to the same block
  // as index iterator.
  bool segment_iter_points_to_real_block_;
  uint64_t prev_segment_offset_ = std::numeric_limits<uint64_t>::max();
  BlockCacheLookupContext lookup_context_;
  BlockPrefetcher block_prefetcher_;

 private:
  friend class BlockBasedTableReaderTestVerifyChecksum_ChecksumMismatch_Test;
};
}  // namespace ROCKSDB_NAMESPACE
