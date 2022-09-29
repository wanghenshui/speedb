//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
struct Options;
class CacheReservationManager;
class InstrumentedMutex;
class InstrumentedCondVar;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // TODO: Need to find an alternative name as it is misleading
  // we start flushes in kStartFlushPercentThreshold / number of parallel
  // flushes
  static constexpr uint64_t kStartFlushPercentThreshold = 80U;

  struct FlushInitiationOptions {
    static constexpr size_t kDfltMaxNumParallelFlushes = 4U;

    FlushInitiationOptions() {}
    size_t max_num_parallel_flushes = kDfltMaxNumParallelFlushes;
  };

  static constexpr bool kDfltAllowStall = false;
  static constexpr bool kDfltInitiateFlushes = true;

 public:
  // Parameters:
  // _buffer_size: _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  //
  // initiate_flushes: if set true, the WBM will proactively request registered
  // DB-s to flush. The mechanism is based on initiating an increasing number of
  // flushes as the memory usage increases. If set false, WBM clients need to
  // call ShouldFlush() and the WBM will indicate if current memory usage merits
  // a flush. Currently the ShouldFlush() mechanism is used only in the
  // write-path of a DB.
  explicit WriteBufferManager(
      size_t _buffer_size, std::shared_ptr<Cache> cache = {},
      bool allow_stall = kDfltAllowStall,
      bool initiate_flushes = kDfltInitiateFlushes,
      const FlushInitiationOptions& flush_initiation_options =
          FlushInitiationOptions());

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    const size_t total = memory_usage();
    const size_t inactive = memory_inactive_.load(std::memory_order_acquire);
    return ((inactive >= total) ? 0 : (total - inactive));
  }

  // Returns the total inactive memory used by memtables.
  size_t immmutable_memtable_memory_usage() const {
    return memory_inactive_.load(std::memory_order_relaxed);
  }

  // Returns the total memory marked to be freed but not yet actually freed
  size_t memtable_memory_being_freed_usage() const {
    return memory_being_freed_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  // Note that the memory_inactive_ and memory_being_freed_ counters
  // are NOT maintained when the WBM is disabled. In addition, memory_used_ is
  // maintained only when enabled or cache is provided. Therefore, if switching
  // from disabled to enabled, these counters will (or may) be invalid or may
  // wraparound
  // REQUIRED: `new_size` > 0
  void SetBufferSize(size_t new_size) {
    assert(new_size > 0);
    [[maybe_unused]] auto was_enabled = enabled();

    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);

    assert(was_enabled == enabled());

    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
    if (enabled()) {
      if (initiate_flushes_) {
        InitFlushInitiationVars(new_size);
      }
    }
  }

  void SetAllowStall(bool new_allow_stall) {
    allow_stall_.store(new_allow_stall, std::memory_order_relaxed);
    MaybeEndWriteStall();
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
  bool ShouldFlush() const {
    if ((initiate_flushes_ == false) && enabled()) {
      if (mutable_memtable_memory_usage() >
          mutable_limit_.load(std::memory_order_relaxed)) {
        return true;
      }
      size_t local_size = buffer_size();
      if (memory_usage() >= local_size &&
          mutable_memtable_memory_usage() >= local_size / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_stall = true during WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally .
  bool ShouldStall() const {
    if (!allow_stall_.load(std::memory_order_relaxed) || !enabled()) {
      return false;
    }

    return IsStallActive() || IsStallThresholdExceeded();
  }

  // Returns true if stall is active.
  bool IsStallActive() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met.
  bool IsStallThresholdExceeded() const {
    return memory_usage() >= buffer_size_;
  }

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  // Freeing 'mem' bytes has actually started.
  // The process may complete successfully and FreeMem() will be called to
  // notifiy successfull completion, or, aborted, and FreeMemCancelled() will be
  // called to notify that.
  void FreeMemBegin(size_t mem);

  // Freeing 'mem' bytes was aborted and that memory is no longer in the process
  // of being freed
  void FreeMemAborted(size_t mem);

  // Freeing 'mem' bytes completed successfully
  void FreeMem(size_t mem);

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall);

  std::string GetPrintableOptions() const;

 public:
  bool IsInitiatingFlushes() const { return initiate_flushes_; }
  const FlushInitiationOptions& GetFlushInitiationOptions() const {
    return flush_initiation_options_;
  }

 public:
  using InitiateFlushRequestCb = std::function<bool(size_t min_size_to_flush)>;

  void RegisterFlushInitiator(void* initiator, InitiateFlushRequestCb request);
  void DeregisterFlushInitiator(void* initiator);

  void FlushStarted(bool wbm_initiated);
  void FlushEnded(bool wbm_initiated);

 public:
  size_t TEST_GetNumFlushesToInitiate() const {
    return num_flushes_to_initiate_;
  }
  size_t TEST_GetNumRunningFlushes() const { return num_running_flushes_; }
  size_t TEST_GetNextCandidateInitiatorIdx() const {
    return next_candidate_initiator_idx_;
  }

  void TEST_WakeupFlushInitiationThread();

 private:
  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_ = 0U;
  // Memory that has been scheduled to free.
  std::atomic<size_t> memory_inactive_ = 0U;
  // Memory that in the process of being freed
  std::atomic<size_t> memory_being_freed_ = 0U;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  std::list<StallInterface*> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex mu_;
  std::atomic<bool> allow_stall_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::atomic<bool> stall_active_;

  // Return the new memory usage
  size_t ReserveMemWithCache(size_t mem);
  size_t FreeMemWithCache(size_t mem);

 private:
  struct InitiatorInfo {
    void* initiator = nullptr;
    InitiateFlushRequestCb cb;
  };

  static constexpr uint64_t kInvalidInitiatorIdx =
      std::numeric_limits<uint64_t>::max();

 private:
  void InitFlushInitiationVars(size_t quota);
  void InitiateFlushesThread();
  bool InitiateAdditionalFlush();
  void WakeUpFlushesThread();
  void TerminateFlushesThread();
  void RecalcFlushInitiationSize();
  void ReevaluateNeedForMoreFlushesNoLockHeld(size_t curr_memory_used);
  void ReevaluateNeedForMoreFlushesLockHeld(size_t curr_memory_used);
  uint64_t FindInitiator(void* initiator) const;

  void WakeupFlushInitiationThreadNoLockHeld();
  void WakeupFlushInitiationThreadLockHeld();

  // Heuristic to decide if another flush is needed taking into account
  // only memory issues (ignoring number of flushes issues).
  // May be called NOT under the flushes_mu_ lock
  //
  // NOTE: Memory is not necessarily freed at the end of a flush for various
  // reasons. For now, the memory is considered dirty until it is actually
  // freed. For that reason we do NOT initiate another flush immediatley once a
  // flush ends, we wait until the total unflushed memory (curr_memory_used -
  // memory_being_freed_) exceeds a threshold.
  bool ShouldInitiateAnotherFlushMemOnly(size_t curr_memory_used) const {
    return (curr_memory_used - memory_being_freed_ >=
                additional_flush_step_size_ / 2 &&
            curr_memory_used >= additional_flush_initiation_size_);
  }

  // This should be called only under the flushes_mu_ lock
  bool ShouldInitiateAnotherFlush(size_t curr_memory_used) const {
    return (((num_running_flushes_ + num_flushes_to_initiate_) <
             flush_initiation_options_.max_num_parallel_flushes) &&
            ShouldInitiateAnotherFlushMemOnly(curr_memory_used));
  }

  void UpdateNextCandidateInitiatorIdx();
  bool IsInitiatorIdxValid(uint64_t initiator_idx) const;

 private:
  // Flush Initiation Mechanism Data Members

  const bool initiate_flushes_ = false;
  const FlushInitiationOptions flush_initiation_options_ =
      FlushInitiationOptions();

  // Collection of registered initiators
  std::vector<InitiatorInfo> flush_initiators_;
  // Round-robin index of the next candidate flushes initiator
  uint64_t next_candidate_initiator_idx_ = kInvalidInitiatorIdx;

  // Number of flushes actually running (regardless of who initiated them)
  std::atomic<size_t> num_running_flushes_ = 0U;
  // Number of additional flushes to initiate the mechanism deems necessary
  std::atomic<size_t> num_flushes_to_initiate_ = 0U;
  // Threshold (bytes) from which to start initiating flushes
  size_t flush_initiation_start_size_ = 0U;
  size_t additional_flush_step_size_ = 0U;
  std::atomic<size_t> additional_flush_initiation_size_ = 0U;
  // Min estimated size (in bytes) of the mutable memtable(s) for an initiator
  // to start a flush when requested
  size_t min_mutable_flush_size_ = 0U;

  // Trying to include instumented_mutex.h results in a compilation error
  // so only forward declaration + unique_ptr instead of having a member by
  // value
  std::unique_ptr<InstrumentedMutex> flushes_mu_;
  std::unique_ptr<InstrumentedMutex> flushes_initiators_mu_;
  // Used to wake up the flushes initiation thread when it has work to do
  std::unique_ptr<InstrumentedCondVar> flushes_wakeup_cv_;
  // Allows the flush initiation thread to wake up only when there is truly
  // reason to wakeup. See the thread's code for more details
  bool new_flushes_wakeup_ = false;

  std::thread flushes_thread_;
  bool terminate_flushes_thread_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
