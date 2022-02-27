#pragma once

#include "table/block_based/index_reader_common.h"

namespace ROCKSDB_NAMESPACE {

// TODO - Add Documentation
class SpdbTwoLevelndexReader : public BlockBasedTable::IndexReaderCommon {
 public:
  // Read the partition index from the file and create an instance for
  // `SpdbTwoLevelndexReader`.
  // On success, index_reader will be populated; otherwise it will remain
  // unmodified.
  static Status Create(const BlockBasedTable* table, const ReadOptions& ro,
                       FilePrefetchBuffer* prefetch_buffer, bool use_cache,
                       bool prefetch, bool pin,
                       BlockCacheLookupContext* lookup_context,
                       std::unique_ptr<IndexReader>* index_reader);

  // return a two-level iterator: first level is on the partition index
  InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool /* disable_prefix_seek */,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override;

  Status CacheDependencies(const ReadOptions& ro, bool pin) override;
  size_t ApproximateMemoryUsage() const override;

 private:
  SpdbTwoLevelndexReader(const BlockBasedTable* t,
                         CachableEntry<Block>&& index_block)
      : IndexReaderCommon(t, std::move(index_block)) {}
};

}  // namespace ROCKSDB_NAMESPACE
