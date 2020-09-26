#pragma once

#include "common/concurrent_bitmap.h"
#include "common/macros.h"
#include "storage/storage_defs.h"
#include <cstddef>
#include <type_traits>
#include <vector>

namespace noisepage {

namespace storage {
/**
 * mini block存储一列数据
 * ----------------------------------------------------
 * | null-bitmap (pad up to byte) | val1 | val2 | ... |
 * ----------------------------------------------------
 */
struct MiniBlock {
public:
  MiniBlock() = delete;
  DISALLOW_COPY_AND_MOVE(MiniBlock);
  ~MiniBlock() = delete;
  RawConcurrentBitmap *NullBitmap() {
    return reinterpret_cast<RawConcurrentBitmap *>(varlen_contents_);
  }

  byte *ColumnStart(const BlockLayout &layout) {
    return varlen_contents_ + BitmapSize(layout.num_slots_);
  }
  byte varlen_contents_[0]{};
};

/**
 * ---------------------------------------------------------------------
 * | block_id | num_records | num_slots | attr_offsets[num_attributes] | //
 * 32-bit fields
 * ---------------------------------------------------------------------
 * | num_attrs (16-bit) | attr_sizes[num_attr] (8-bit) |   ...content  |
 * ---------------------------------------------------------------------
 */
struct Block {
  Block() = delete;
  DISALLOW_COPY_AND_MOVE(Block);
  ~Block() = delete;

  uint32_t &NumSlots() {
    return *reinterpret_cast<uint32_t *>(varlen_contents_);
  }

  uint32_t *AttrOffsets() { return &NumSlots() + 1; }

  uint16_t &NumAttrs(const BlockLayout &layout) {
    return *reinterpret_cast<uint16_t *>(AttrOffsets() + layout.num_cols_);
  }

  uint8_t *AttrSizes(const BlockLayout &layout) {
    return reinterpret_cast<uint8_t *>(&NumAttrs(layout) + 1);
  }

  MiniBlock *Column(uint16_t col_offset) {
    byte *head = reinterpret_cast<byte *>(this) + AttrOffsets()[col_offset];
    return reinterpret_cast<MiniBlock *>(head);
  }

  uint32_t block_id_;
  uint32_t num_records_;
  byte varlen_contents_[0];
};

class TupleAccessStrategy {
public:
  TupleAccessStrategy(const BlockLayout &layout) : layout_(layout) {}

  RawConcurrentBitmap *ColumnNullBitmap(RawBlock *block, uint16_t col_offset) {
    return reinterpret_cast<Block *>(block)->Column(col_offset)->NullBitmap();
  }

  bool Allocate(RawBlock *block, TupleSlot &slot) {
    auto *null_bitmap = ColumnNullBitmap(block, 0);
    for (uint32_t i = 0; i < layout_.num_slots_; i++) {
      if (null_bitmap->Flip(i, false)) {
        slot = TupleSlot(block, i);
        return true;
      }
    }
    return false;
  }

  byte *ColumnAt(RawBlock *block, uint16_t col_id, uint32_t offset) {
    byte *column_start = reinterpret_cast<Block *>(block)->Column(col_id)->ColumnStart(layout_);
    return column_start + offset * layout_.attr_sizes_[col_id];
  }

  byte *AccessWithNullCheck(RawBlock *block, uint16_t col_id, uint32_t offset) {
    if (!ColumnNullBitmap(block, col_id)->Test(offset)) {
      return nullptr;
    }
    return ColumnAt(block, col_id, offset);
  }

  byte *AccessForceNotNull(RawBlock *block, uint16_t col_id, uint32_t offset) {
    ColumnNullBitmap(block, col_id)->Flip(offset, false);
    return ColumnAt(block, col_id, offset);
  }

  const BlockLayout &GetBlockLayout() const { return layout_; }
private:
  const BlockLayout layout_;
};
} // namespace storage

} // namespace noisepage
