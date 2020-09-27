#include "storage/data_table.h"
#include "storage/storage_util.h"

namespace noisepage::storage {

DataTable::DataTable(BlockStore store, BlockLayout layout) : block_store_(store), accessor_(layout) {
  RawBlock *new_block = block_store_.Get();
  InitializeRawBlock(new_block, layout, 0);
  insertion_head_ = new_block;
  blocks_.PushBack(new_block);
} 

void DataTable::Select(const TupleSlot &slot, ProjectedRow *out_buffer) {
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
  }
}

TupleSlot DataTable::Insert(const ProjectedRow &redo) {
  TupleSlot result;
  accessor_.Allocate(insertion_head_, result);
  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    StorageUtil::CopyAttrFromProjection(redo, accessor_, result, i);
  }
  return result;
}

} // namespace noisepage::storage
