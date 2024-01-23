// Copyright (C) 2023 Speedb Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// #include "db/dbformat.h"
#include "db/db_impl/spdb_gs_del_list.h"

namespace ROCKSDB_NAMESPACE {
// Forward Declarations
struct RangeTombstone;
class FragmentedRangeTombstoneIterator;

namespace spdb_gs {

enum class RelativePos { BEFORE, OVERLAP, AFTER };

void PrintFragmentedRangeDels(
    const std::string& title,
    ROCKSDB_NAMESPACE::FragmentedRangeTombstoneIterator* iter);

RelativePos CompareRangeTsToUserKey(const RangeTombstone& range_ts,
                                    const Slice& user_key,
                                    const Comparator* comparator);

RelativePos CompareDelElemToUserKey(const GlobalDelList::DelElement& del_elem,
                                    const Slice& user_key,
                                    const Comparator* comparator);

RelativePos CompareDelElemToRangeTs(const GlobalDelList::DelElement& del_elem,
                                    const RangeTombstone& range_ts,
                                    const Comparator* comparator);

}  // namespace spdb_gs
}  // namespace ROCKSDB_NAMESPACE
