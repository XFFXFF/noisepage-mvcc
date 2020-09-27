#pragma once
#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "common/object_pool.h"
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
  TupleSlot() : bytes_(0) {}
  TupleSlot(RawBlock *block, uint32_t offset)
      : bytes_((uintptr_t)(block) | static_cast<uintptr_t>(offset)) {
    assert(!(uintptr_t(block) & static_cast<uintptr_t>(BLOCK_SIZE - 1)));
    assert(offset < BLOCK_SIZE);
  }

  RawBlock *GetBlock() const {
    return reinterpret_cast<RawBlock *>(
        bytes_ & ~static_cast<uintptr_t>(BLOCK_SIZE - 1));
  }

  uint32_t GetOffset() const {
    return static_cast<uint32_t>(bytes_ &
                                 static_cast<uintptr_t>(BLOCK_SIZE - 1));
  }

private:
  uintptr_t bytes_;
};

using BlockStore = ObjectPool<RawBlock>;

/**
 * projected row可能只是包含一个record的部分列
 * A projected row is a partial row image of a tuple. It also encodes
 * a projection list that allows for reordering of the columns. Its in-memory
 * layout:
 * ------------------------------------------------------------------------
 * | num_cols | col_id1 | col_id2 | ... | val1_offset | val2_offset | ... |
 * ------------------------------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ...                     |
 * ------------------------------------------------------------------------
 * Warning, 0 means null in the null-bitmap
 *
 * The projection list is encoded as position of col_id -> col_id. For example:
 *
 * ---------------------------------------------------
 * | 3 | 1 | 0 | 2 | 0 | 4 | 8 | 0xC0 | 721 | 15 | x |
 * ---------------------------------------------------
 * Would be the row: { 0 -> 15, 1 -> 721, 2 -> nul}
 */
class ProjectedRow {
public:
  ProjectedRow() = delete;
  DISALLOW_COPY_AND_MOVE(ProjectedRow);
  ~ProjectedRow() = delete;

  static uint32_t Size(const BlockLayout &layout,
                       const std::vector<uint16_t> &col_ids);

  static ProjectedRow *
  InitializeProjectedRow(byte *head, const BlockLayout &layout,
                         const std::vector<uint16_t> &col_ids);

  uint16_t &NumColumns() { return num_cols_; }

  const uint16_t &NumColumns() const { return num_cols_; }

  uint16_t *ColumnIds() {
    return reinterpret_cast<uint16_t *>(varlen_contents_);
  }

  const uint16_t *ColumnIds() const {
    return reinterpret_cast<const uint16_t *>(varlen_contents_);
  }

  uint32_t *AttrValueOffset() {
    return reinterpret_cast<uint32_t *>(ColumnIds() + num_cols_);
  }

  const uint32_t *AttrValueOffset() const {
    return reinterpret_cast<const uint32_t *>(ColumnIds() + num_cols_);
  }

  RawConcurrentBitmap *NullBitmap() {
    return reinterpret_cast<RawConcurrentBitmap *>(AttrValueOffset() +
                                                   num_cols_);
  }

  const RawConcurrentBitmap *NullBitmap() const {
    return reinterpret_cast<const RawConcurrentBitmap *>(AttrValueOffset() +
                                                         num_cols_);
  }

  byte *AccessWithNullCheck(uint16_t offset) {
    if (!NullBitmap()->Test(offset)) {
      return nullptr;
    }
    return reinterpret_cast<byte *>(this) + AttrValueOffset()[offset];
  }

  const byte *AccessWithNullCheck(uint16_t offset) const {
    if (!NullBitmap()->Test(offset)) {
      return nullptr;
    }
    return reinterpret_cast<const byte *>(this) + AttrValueOffset()[offset];
  }

  byte *AccessForceNotNull(uint16_t offset) {
    NullBitmap()->Flip(offset, false);
    return reinterpret_cast<byte *>(this) + AttrValueOffset()[offset];
  }

  void SetNull(uint16_t offset) { NullBitmap()->Flip(offset, true); }

private:
  uint16_t num_cols_;
  byte varlen_contents_[0];
};
} // namespace storage

} // namespace noisepage
