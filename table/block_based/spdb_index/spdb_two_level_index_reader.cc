#include "file/random_access_file_reader.h"
#include "table/block_based/spdb_index/spdb_segments_Index_iterator.h"
#include "table/block_based/spdb_index/spdb_two_level_index_reader.h"

namespace ROCKSDB_NAMESPACE {
Status SpdbTwoLevelndexReader::Create(
    const BlockBasedTable* table, const ReadOptions& ro,
    FilePrefetchBuffer* prefetch_buffer, bool use_cache, bool prefetch,
    bool pin, BlockCacheLookupContext* lookup_context,
    std::unique_ptr<IndexReader>* index_reader) {
  assert(table != nullptr);
  assert(table->get_rep());
  assert(!pin || prefetch);
  assert(index_reader != nullptr);

  // TODO - Decide in the next phases if we require use_cache and pin to both be true
  // as we intend our top-level index to be pinned in cache always
  // The current code is (mostly) copied from Partitioned Index

  CachableEntry<Block> index_block;
  if (prefetch || !use_cache) {
    const Status s =
        ReadIndexBlock(table, prefetch_buffer, ro, use_cache,
                       /*get_context=*/nullptr, lookup_context, &index_block);
    if (!s.ok()) {
      return s;
    }

    // TODO - We would never want this to happen - we would like our top-level index
    // block to be pinned in cache (=> we must enforce the existence of a block cache when working with Speedb's Index)
    if (use_cache && !pin) {
      index_block.Reset();
    }
  }

  index_reader->reset(
      new SpdbTwoLevelndexReader(table, std::move(index_block)));

  return Status::OK();
}

InternalIteratorBase<IndexValue>* SpdbTwoLevelndexReader::NewIterator(
    const ReadOptions& read_options, bool /* disable_prefix_seek */,
    IndexBlockIter* iter, GetContext* get_context,
    BlockCacheLookupContext* lookup_context) {
  // TODO - Do we want to support spdb index without io?
  const bool no_io = (read_options.read_tier == kBlockCacheTier);
  CachableEntry<Block> index_block;
  const Status s =
      GetOrReadIndexBlock(no_io, get_context, lookup_context, &index_block);
  if (!s.ok()) {
    if (iter != nullptr) {
      iter->Invalidate(s);
      return iter;
    }

    return NewErrorInternalIterator<IndexValue>(s);
  }

  const BlockBasedTable::Rep* rep = table()->rep_;
  InternalIteratorBase<IndexValue>* it = nullptr;

  Statistics* kNullStats = nullptr;
  ReadOptions ro;
  ro.fill_cache = read_options.fill_cache;
  ro.deadline = read_options.deadline;
  ro.io_timeout = read_options.io_timeout;
  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  std::unique_ptr<InternalIteratorBase<IndexValue>> index_iter(
      index_block.GetValue()->NewIndexIterator(
          internal_comparator()->user_comparator(),
          rep->get_global_seqno(BlockType::kIndex), nullptr, kNullStats, true,
          index_has_first_key(), index_key_includes_seq(),
          index_value_is_full()));

  it = new SpdbSegmentsIndexIterator(
      table(), ro, *internal_comparator(), std::move(index_iter),
      lookup_context ? lookup_context->caller
                     : TableReaderCaller::kUncategorized);

  assert(it != nullptr);
  index_block.TransferTo(it);

  return it;

  // TODO(myabandeh): Update TwoLevelIterator to be able to make use of
  // on-stack BlockIter while the state is on heap. Currentlly it assumes
  // the first level iter is always on heap and will attempt to delete it
  // in its destructor.
}

Status SpdbTwoLevelndexReader::CacheDependencies(const ReadOptions& ro,
                                                 bool /*pin*/) {
  // Before read partitions, prefetch them to avoid lots of IOs
  BlockCacheLookupContext lookup_context{TableReaderCaller::kPrefetch};
  const BlockBasedTable::Rep* rep = table()->rep_;
  IndexBlockIter biter;
  BlockHandle handle;
  Statistics* kNullStats = nullptr;

  CachableEntry<Block> index_block;
  Status s = GetOrReadIndexBlock(false /* no_io */, nullptr /* get_context */,
                                 &lookup_context, &index_block);
  if (!s.ok()) {
    return s;
  }

  // We don't return pinned data from index blocks, so no need
  // to set `block_contents_pinned`.
  index_block.GetValue()->NewIndexIterator(
      internal_comparator()->user_comparator(),
      rep->get_global_seqno(BlockType::kIndex), &biter, kNullStats, true,
      index_has_first_key(), index_key_includes_seq(), index_value_is_full());
  // Index partitions are assumed to be consecuitive. Prefetch them all.
  // Read the first block offset
  biter.SeekToFirst();
  if (!biter.Valid()) {
    // Empty index.
    return biter.status();
  }
  handle = biter.value().handle;
  uint64_t prefetch_off = handle.offset();

  // Read the last block's offset
  biter.SeekToLast();
  if (!biter.Valid()) {
    // Empty index.
    return biter.status();
  }
  handle = biter.value().handle;
  uint64_t last_off = handle.offset() + block_size(handle);
  uint64_t prefetch_len = last_off - prefetch_off;
  std::unique_ptr<FilePrefetchBuffer> prefetch_buffer;
  rep->CreateFilePrefetchBuffer(0, 0, &prefetch_buffer,
                                false /*Implicit auto readahead*/);
  IOOptions opts;
  s = rep->file->PrepareIOOptions(ro, opts);
  if (s.ok()) {
    s = prefetch_buffer->Prefetch(opts, rep->file.get(), prefetch_off,
                                  static_cast<size_t>(prefetch_len));
  }
  if (!s.ok()) {
    return s;
  }

  // After prefetch, read the partitions one by one
  biter.SeekToFirst();
  for (; biter.Valid(); biter.Next()) {
    handle = biter.value().handle;
    CachableEntry<Block> block;
    // TODO: Support counter batch update for partitioned index and
    // filter blocks
    s = table()->MaybeReadBlockAndLoadToCache(
        prefetch_buffer.get(), ro, handle, UncompressionDict::GetEmptyDict(),
        /*wait=*/true, &block, BlockType::kIndex, /*get_context=*/nullptr,
        &lookup_context, /*contents=*/nullptr);

    if (!s.ok()) {
      return s;
    }
  }
  return biter.status();
}

size_t SpdbTwoLevelndexReader::ApproximateMemoryUsage() const {
  size_t usage = ApproximateIndexBlockMemoryUsage();
  
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  usage += malloc_usable_size(const_cast<SpdbTwoLevelndexReader*>(this));
#else
  usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE

  // TODO(myabandeh): more accurate estimate of partition_map_ mem usage
  return usage;
}

}  // namespace ROCKSDB_NAMESPACE
