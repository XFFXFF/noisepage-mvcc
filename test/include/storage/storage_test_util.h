#pragma once
#include <vector>
#include "storage/storage_defs.h"
#include "common/test_util.h"
#include "storage/tuple_access_strategy_test_util.h"
#include "storage/storage_util.h"

namespace noisepage
{
namespace testutil
{
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator, uint16_t max_col=UINT16_MAX) {
  uint16_t num_attrs = std::uniform_int_distribution<>(2, max_col)(generator);
  std::vector<uint8_t> possible_attr_sizes = {1, 2, 4, 8}; 
  std::vector<uint8_t> attr_sizes(num_attrs);
  attr_sizes[0] = 8; // 第一个attr总是version_ptr
  for (auto i = 1; i < num_attrs; i++) {
    auto it = UniformRandomElement(possible_attr_sizes, generator);
    attr_sizes[i] = *it;
  }
  return {num_attrs, attr_sizes};
}  

template<typename Random>
void FillWithRandomBytes(uint32_t num_bytes, byte *out, Random &generator) {
  std::uniform_int_distribution dist(0, UINT8_MAX);
  for (uint32_t i = 0; i < num_bytes; i++) {
    out[i] = static_cast<byte>(dist(generator));
  }
}

template<typename Random>
void PopulateRandomRow(storage::ProjectedRow *row, const storage::BlockLayout layout, 
                       const double null_bias, Random &generator) {
  for (uint16_t i = 0; i < row->NumColumns(); i++) {
    uint16_t col_id = row->ColumnIds()[i];
    std::bernoulli_distribution coin(1 - null_bias);

    if (coin(generator)) {
      uint32_t num_bytes = layout.attr_sizes_[col_id];
      byte *out = row->AccessForceNotNull(i);
      FillWithRandomBytes(num_bytes, out, generator);
    }
  }
}

void PrintRow(storage::ProjectedRow *row, const storage::BlockLayout &layout) {
  for (uint16_t i = 0; i < row->NumColumns(); i++) {
    uint16_t col_id = row->ColumnIds()[i];
    byte *out = row->AccessWithNullCheck(i);
    if (out) {
      printf("col_id: %u is %lx \n", col_id, 
            storage::StorageUtil::ReadBytes(layout.attr_sizes_[i], out));
    } else {
      printf("col_id: %u is NULL\n", col_id);
    }
  }
}

std::vector<uint16_t> ProjectionListAllColumns(const storage::BlockLayout &layout) {
  std::vector<uint16_t> col_ids(layout.num_cols_ - 1);
  for (uint16_t i = 1; i < layout.num_cols_; i++) {
    col_ids[i - 1] = i;
  }
  return col_ids;
}

bool ProjectionListEqual(const storage::BlockLayout &layout,
                         const storage::ProjectedRow &one,
                         const storage::ProjectedRow &other) {
  if (one.NumColumns() != other.NumColumns()) return false;
  for (uint16_t i = 0; i < one.NumColumns(); i++) {
    if (one.ColumnIds()[i] != other.ColumnIds()[i]) return false;
  }

  for (uint16_t i = 0; i < one.NumColumns(); i++) {
    uint16_t col_id = one.ColumnIds()[i];
    uint8_t attr_size = layout.attr_sizes_[col_id];
    auto *one_pos = one.AccessWithNullCheck(i);
    auto *other_pos = other.AccessWithNullCheck(i);

    if (one_pos == nullptr || other_pos == nullptr) {
      if (one_pos == other_pos) {
        continue;
      } else {
        return false;
      }
    }

    uint64_t one_val = storage::StorageUtil::ReadBytes(attr_size, one_pos);    
    uint64_t other_val = storage::StorageUtil::ReadBytes(attr_size, other_pos);
    EXPECT_EQ(one_val, other_val);
    if (one_val != other_val) return false;
  }
  return true;
}

} // namespace testutil

  
} // namespace noisepage
