#pragma once
#include "common/macros.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace noisepage {
using byte = std::byte;

namespace storage {
constexpr uint32_t BLOCK_SIZE = 1048576u;

struct BlockLayout {
  BlockLayout(uint16_t num_attrs, std::vector<uint8_t> attr_sizes)
      : num_cols_(num_attrs), attr_sizes_(std::move(attr_sizes)),
        num_slots_(NumSlots()), header_size_(HeaderSize()),
        tuple_size_(TupleSize()) {}
  const uint16_t num_cols_;
  const std::vector<uint8_t> attr_sizes_;
  const uint32_t num_slots_;
  const uint32_t header_size_;
  const uint32_t tuple_size_;

private:
  uint32_t HeaderSize() const {
    return sizeof(uint32_t) * 3           // block_id, num_records, num_slots
           + sizeof(uint32_t) * num_cols_ // attr_offsets
           + sizeof(uint16_t)             // num_attrs
           + sizeof(uint8_t) * num_cols_; // attr_sizes
  }

  uint32_t TupleSize() const {
    uint32_t tuple_size = 0;
    for (auto attr_size : attr_sizes_) {
      tuple_size += attr_size;
    }
    return tuple_size;
  }

  uint32_t NumSlots() {
    return 8 * ((BLOCK_SIZE)-HeaderSize()) / (8 * TupleSize() + num_cols_) - 1;
  }
};

class RawBlock {
public:
  byte content_[BLOCK_SIZE];
} __attribute__((aligned(BLOCK_SIZE)));

void InitializeRawBlock(RawBlock *raw, const BlockLayout &layout,
                        uint32_t block_id);

class TupleSlot {
public:
  TupleSlot(RawBlock *block, uint32_t offset)
      : bytes_((uintptr_t)(block) | static_cast<uintptr_t>(offset)) {
    assert(!(uintptr_t(block) & static_cast<uintptr_t>(BLOCK_SIZE - 1)));
    assert(offset < BLOCK_SIZE);
  }

  RawBlock *GetBlock() {
    return reinterpret_cast<RawBlock *>(
        bytes_ & static_cast<uintptr_t>(~(BLOCK_SIZE - 1)));
  }

  uint32_t GetOffset() {
    return static_cast<uint32_t>(bytes_ &
                                 static_cast<uintptr_t>(BLOCK_SIZE - 1));
  }

private:
  uintptr_t bytes_;
};

/**
 * ------------------------------------------------------------------------
 * | num_cols | col_id1 | col_id2 | ... | val1_offset | val2_offset | ... |
 * ------------------------------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ...                     |
 * ------------------------------------------------------------------------
 */
class ProjectedRow {
public:
  ProjectedRow() = delete;
  DISALLOW_COPY_AND_MOVE(ProjectedRow);
  ~ProjectedRow() = delete;

  static ProjectedRow *
  InitializeProjectedRow(byte *head, const std::vector<uint16_t> col_ids,
                         const BlockLayout &layout);

private:
  uint16_t num_cols_;
  byte varlen_contents_[0];
};
} // namespace storage

} // namespace noisepage
