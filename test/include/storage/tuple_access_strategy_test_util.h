#pragma once

#include <cassert>
#include <random>
#include <unordered_map>
#include "storage/tuple_access_strategy.h"
#include "gtest/gtest.h"

namespace noisepage
{
namespace testutil
{
uint64_t ReadByteValue(uint8_t attr_size, byte *pos) {
  switch (attr_size) {
  case 1:
    return *reinterpret_cast<uint8_t *>(pos);
    break;
  case 2: 
    return *reinterpret_cast<uint16_t *>(pos);
    break;
  case 4:
    return *reinterpret_cast<uint32_t *>(pos);
    break;
  case 8:
    return *reinterpret_cast<uint64_t *>(pos);
  default:
    assert(false);
    break;
  }
}

void WriteByteValue(uint8_t attr_size, uint64_t val, byte *pos) {
  switch (attr_size) {
  case 1:
    *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
    break;
  case 2:
    *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
    break;
  case 4:
    *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
    break;
  case 8:
    *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
    break;
  default:
    assert(false);
    break;
  }
}

struct FakeRawTuple {
  FakeRawTuple(const storage::BlockLayout layout, byte *contents) 
      : layout_(layout), contents_(contents) {
    uint32_t pos = 0;
    for (auto attr_size : layout_.attr_sizes_) {
      attr_offsets_.push_back(pos);
      pos += attr_size;
    }
  }
  
  // 这个析构函数非常的奇怪，这样内存很容易泄露
  ~FakeRawTuple() {
    delete contents_;
  }

  uint64_t Attribute(uint16_t col_id) const {
    byte *pos = contents_ + attr_offsets_[col_id];
    return ReadByteValue(layout_.attr_sizes_[col_id], pos);
  }

  const storage::BlockLayout layout_;
  std::vector<uint32_t> attr_offsets_;
  byte *contents_;
};

template<typename Random>
byte *RandomTupleContent(const storage::BlockLayout &layout, Random &generator) {
  byte *contents = new byte[layout.tuple_size_];
  std::uniform_int_distribution<uint8_t> dist(0, UINT8_MAX);
  for (uint16_t i = 0; i < layout.num_cols_; i++) {
    contents[i] = static_cast<byte>(dist(generator));
  }
  return contents;
}

void InsertTuple(const FakeRawTuple &tuple,
                 const storage::BlockLayout &layout,
                 storage::TupleAccessStrategy &tested,
                 storage::Block *block,
                 uint32_t offset) {
  for (uint16_t i = 0; i < layout.num_cols_; i++) {
    auto *pos = tested.AccessWithNullCheck(block, i, offset);
    uint64_t val = tuple.Attribute(i);
    WriteByteValue(layout.attr_sizes_[i], val, pos);
  }
}

template<typename Random>
void TryInsertFakeTuple(const storage::BlockLayout &layout,
                        storage::TupleAccessStrategy &tested,
                        storage::Block *block,
                        std::unordered_map<uint32_t, FakeRawTuple> &tuples,
                        Random &generator) {
  uint32_t offset;
  EXPECT_TRUE(tested.Allocate(block, offset));
  EXPECT_TRUE(tested.ColumnNullBitmap(block, 0)->Test(offset));

  auto result = tuples.emplace(
    std::piecewise_construct,
    std::forward_as_tuple(offset),
    std::forward_as_tuple(layout, RandomTupleContent(layout, generator));
  )

  EXPECT_TRUE(result.second);
  InsertTuple(result.first->second, layout, tested, block, offset);
}
  
} // namespace testutil

  
} // namespace noisepage
