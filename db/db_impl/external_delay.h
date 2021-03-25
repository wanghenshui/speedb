#pragma once

#include <math.h>
#include <unistd.h>

#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class ExternalDelay {
 public:
  ExternalDelay() : delayPerByte_(0), increment_(0) {}

  static void setExternalDelay(size_t maxRate, double increment) {
    s_externalDelay.setRate(maxRate, increment);
  }
  static void reduceExternalDelay() { s_externalDelay.delayPerByte_ = 0; }
  static void enforce(Env *env, size_t numBytes) {
    s_externalDelay.enforceDelay(env, numBytes);
  }
  static size_t getDelay(size_t numBytes) {
    return s_externalDelay.delayPerByte_ * numBytes;
  }

 private:
  static const size_t kMicrosInSec = 1000000;
  void setRate(size_t rate, double increment) {
    if (rate == 0) {
      delayPerByte_ = 0;
    } else {
      delayPerByte_ = 1.0 * kMicrosInSec / rate;
    }
    increment_ = increment;
  }
  void enforceDelay(Env *env, size_t numBytes) {
    size_t delay = getDelay(numBytes);
    if (delay == 0) {
      return;
    }
    if (delay < 10) {
      if ((size_t)rand() % 10000 < delay * 1000) {
        env->SleepForMicroseconds(10);
      }
    } else {
      if (delay > 100000) {
        delay = 100000;  // do not sleep more than 0.1 sec
      }
      env->SleepForMicroseconds(size_t(delay));
    }
    delayPerByte_ *= pow(increment_, numBytes);
  }

 private:
  static ExternalDelay s_externalDelay;
  double delayPerByte_;
  double increment_;
};

};  // namespace ROCKSDB_NAMESPACE
