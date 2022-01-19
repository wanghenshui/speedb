#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>

#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

class ExternalDelay {
 public:
  ExternalDelay(std::shared_ptr<SystemClock> clock)
      : clock_(std::move(clock)), delay_per_byte_nanos_(0) {}

  void Enforce(size_t byte_count);

  bool Reset() { return SetDelayWriteRate(0) != 0; }

  size_t SetDelayWriteRate(size_t rate);

 private:
  using Nanoseconds = std::chrono::nanoseconds;

  static constexpr auto kNanosPerSec =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::seconds(1)).count();

  static constexpr auto kSleepNanosMin =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::microseconds(100))
          .count();
  static constexpr auto kSleepNanosMax =
      std::chrono::duration_cast<Nanoseconds>(std::chrono::seconds(1)).count();

  std::shared_ptr<SystemClock> clock_;
  std::atomic<double> delay_per_byte_nanos_;
  std::atomic<size_t> next_request_time_;
};

inline void ExternalDelay::Enforce(size_t byte_count) {
  if (delay_per_byte_nanos_ > 0) {
    const auto start_time = clock_->NowNanos();
    const auto delay_mul = 1.0 + (1.0 / kNanosPerSec * byte_count);
    const auto current_delay =
        delay_per_byte_nanos_.load(std::memory_order_acquire);
    // We just need the delay per byte to be written atomically, but we don't
    // really care if another thread wins and sets the delay that it calculated.
    delay_per_byte_nanos_.store(current_delay * delay_mul,
                                std::memory_order_release);

    const auto added_delay = size_t(byte_count * current_delay);
    const auto request_time =
        added_delay +
        next_request_time_.fetch_add(added_delay, std::memory_order_relaxed);
    const auto sleep_time =
        std::min(int(request_time) - int(start_time), int(kSleepNanosMax));
    if (sleep_time > 0 && sleep_time > kSleepNanosMin) {
      const auto sleep_micros =
          std::chrono::duration_cast<std::chrono::microseconds>(
              Nanoseconds(sleep_time))
              .count();
      clock_->SleepForMicroseconds(int(sleep_micros));
    }
  }
}

inline size_t ExternalDelay::SetDelayWriteRate(size_t new_rate) {
  double old_delay = 0;
  if (new_rate == 0) {
    old_delay = delay_per_byte_nanos_.exchange(0, std::memory_order_release);
  } else {
    next_request_time_.store(clock_->NowNanos(), std::memory_order_release);
    old_delay = delay_per_byte_nanos_.exchange(1.0 * kNanosPerSec / new_rate,
                                               std::memory_order_release);
  }
  return old_delay == 0 ? 0 : static_cast<size_t>(kNanosPerSec / old_delay);
}

};  // namespace ROCKSDB_NAMESPACE
