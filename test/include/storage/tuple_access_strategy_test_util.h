#pragma once

#include <cassert>
#include <random>
#include <unordered_map>
#include "storage/tuple_access_strategy.h"
#include "gtest/gtest.h"
#include "storage/storage_util.h"

namespace noisepage {
namespace testutil {

template<typename Random>
void RandomTupleContent(const storage::BlockLayout &layout, byte *contents, Random &generator) {
  std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
  for (uint16_t i = 0; i < layout.num_cols_; i++) {
    contents[i] = static_cast<byte>(dist(generator));
  }
}

struct FakeRawTuple {
  template<typename Random>
  FakeRawTuple(const storage::BlockLayout layout, Random &generator) 
      : layout_(layout), contents_(new byte[layout.tuple_size_]) {
    RandomTupleContent(layout, contents_, generator);
    uint32_t pos = 0;
    for (auto attr_size : layout_.attr_sizes_) {
      attr_offsets_.push_back(pos);
      pos += attr_size;
    }
  }
  
  ~FakeRawTuple() {
    delete[] contents_;
  }

  uint64_t Attribute(uint16_t col_id) const {
    byte *pos = contents_ + attr_offsets_[col_id];
    return storage::StorageUtil::ReadBytes(layout_.attr_sizes_[col_id], pos);
  }

  const storage::BlockLayout layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

void InsertTuple(const FakeRawTuple &tuple,
                 const storage::BlockLayout &layout,
                 storage::TupleAccessStrategy &tested,
                 storage::RawBlock *block,
                 uint32_t offset) {
  storage::TupleSlot slot(block, offset);
  for (uint16_t i = 0; i < layout.num_cols_; i++) {
    auto *pos = tested.AccessForceNotNull(slot, i);
    uint64_t val = tuple.Attribute(i);
    storage::StorageUtil::WriteBytes(layout.attr_sizes_[i], val, pos);
  }
}

template<typename Random>
void TryInsertFakeTuple(const storage::BlockLayout &layout,
                        storage::TupleAccessStrategy &tested,
                        storage::RawBlock *block,
                        std::unordered_map<uint32_t, FakeRawTuple> &tuples,
                        Random &generator) {
  storage::TupleSlot slot;
  EXPECT_TRUE(tested.Allocate(block, slot));
  uint32_t offset = slot.GetOffset();
  EXPECT_TRUE(tested.ColumnNullBitmap(block, 0)->Test(offset));

  auto result = tuples.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(offset),
    std::forward_as_tuple(layout, generator)
  );

  EXPECT_TRUE(result.second);
  InsertTuple(result.first->second, layout, tested, block, offset);
}
  
} // namespace testutil

  
} // namespace noisepage
