#include "storage/tuple_access_strategy.h"

namespace noisepage
{
namespace storage
{
void InitializeRawBlock(RawBlock *raw,
                        const BlockLayout &layout,
                        uint32_t block_id) {
  auto *block = reinterpret_cast<Block *>(raw);
  block->block_id_ = block_id;
  block->num_records_ = 0;
  block->NumSlots() = layout.num_slots_;

  uint32_t attr_offset = layout.HeaderSize();
  for (auto i = 0; i < layout.num_cols_; i++) {
    block->AttrOffsets()[i] = attr_offset;
    attr_offset += layout.attr_sizes_[i] * layout.num_slots_;
  }

  block->NumAttrs(layout) = layout.num_cols_;
  for (auto i = 0; i < layout.num_cols_; i++) {
    block->AttrSizes(layout)[i] = layout.attr_sizes_[i];
  }
}
  
} // namespace storage

  
} // namespace noisepage
