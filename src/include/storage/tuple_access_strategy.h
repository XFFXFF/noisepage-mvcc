#pragma once

#include "common/concurrent_bitmap.h"
#include "storage/storage_defs.h"
#include "common/macros.h"
#include <vector>
#include <type_traits>
#include <cstddef>

namespace noisepage
{
// using byte = std::byte;
#define byte unsigned char

namespace storage
{
struct BlockLayout {
  BlockLayout(uint16_t num_attrs, std::vector<uint8_t> attr_sizes)
      : num_cols_(num_attrs),
        attr_sizes_(std::move(attr_sizes)),
        num_slots_(NumSlots()),
        header_size_(HeaderSize()),
        tuple_size_(TupleSize()) {}
  const uint16_t num_cols_;
  const std::vector<uint8_t> attr_sizes_;
  const uint32_t num_slots_;
  const uint32_t header_size_;
  const uint32_t tuple_size_;

  private:
    uint32_t HeaderSize() const {
      return sizeof(uint32_t) * 3 // block_id, num_records, num_slots
          + sizeof(uint32_t) * num_cols_ // attr_offsets
          + sizeof(uint16_t) // num_attrs
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
      return 8 * ((BLOCK_SIZE) - HeaderSize()) / (8 * TupleSize() + num_cols_) - 1;
    }
};

class RawBlock {
  public:
    byte content_[BLOCK_SIZE];
};

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
      return varlen_contents_ + BitmapSize(layout.num_cols_);
    }
    byte varlen_contents_[0] {};
};

/**
 * ---------------------------------------------------------------------
 * | block_id | num_records | num_slots | attr_offsets[num_attributes] | // 32-bit fields
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

  uint32_t *AttrOffsets() {
    return &NumSlots() + 1;
  }

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

void InitializeRawBlock(RawBlock *raw,
                        const BlockLayout &layout,
                        uint32_t block_id);

class TupleAccessStrategy {
  public:
    TupleAccessStrategy(const BlockLayout &layout) : layout_(layout) {}

    RawConcurrentBitmap *ColumnNullBitmap(Block *block, uint16_t col_offset) {
      return block->Column(col_offset)->NullBitmap();
    }

    bool Allocate(Block *block, uint32_t &offset) {
      auto *null_bitmap = ColumnNullBitmap(block, 0);
      for (auto i = 0; i < layout_.num_slots_; i++) {
        if (null_bitmap->Flip(i, false)) {
          offset = i;
          return true;
        }
      }
      return false;
    }

    byte *ColumnAt(Block *block, uint16_t col_id, uint32_t offset) {
      byte *column_start = block->Column(col_id)->ColumnStart(layout_);
      return column_start + offset * layout_.attr_sizes_[col_id];
    }

    byte *AccessWithNullCheck(Block *block, uint16_t col_id, uint32_t offset) {
      if (!ColumnNullBitmap(block, col_id)->Test(offset)) {
        return nullptr;
      }
      return ColumnAt(block, col_id, offset);
    }

    byte *AccessForceNotNull(Block *block, uint16_t col_id, uint32_t offset) {
      ColumnNullBitmap(block, col_id)->Flip(offset, false);
      return ColumnAt(block, col_id, offset);
    }

  private:
    const BlockLayout layout_;
};
} // namespace storage

  
} // namespace noisepage
