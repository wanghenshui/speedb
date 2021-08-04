// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/memtablerep.h"

namespace ROCKSDB_NAMESPACE {

class HashLocklessRepFactory : public MemTableRepFactory {
 public:
  explicit HashLocklessRepFactory(size_t bucket_count)
      : bucket_count_(bucket_count) {}

  ~HashLocklessRepFactory() override {}

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;
  bool IsInsertConcurrentlySupported() const override { return true; }
  bool CanHandleDuplicatedKey() const override { return true; }

  const char* Name() const override { return "speedb.HashLocklessRepFactory"; }

 private:
  const size_t bucket_count_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
