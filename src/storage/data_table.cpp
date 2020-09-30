#include "storage/data_table.h"
#include "storage/storage_util.h"

#define VERSION_VECTOR_COLUMN_ID 0

namespace noisepage::storage {

DataTable::DataTable(BlockStore &store, BlockLayout layout) : block_store_(store), accessor_(layout) {
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

bool DataTable::Update(const TupleSlot &slot, const ProjectedRow &redo, DeltaRecord *undo) {
  assert(redo.NumColumns() == undo->Delta()->NumColumns());

  DeltaRecord *version_ptr = ReadVersionPtr(slot);

  undo->next_ = version_ptr;

  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, undo->Delta(), i);
  }

  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    StorageUtil::CopyAttrFromProjection(redo, accessor_, slot, i);
  }
  return true;
}

TupleSlot DataTable::Insert(const ProjectedRow &redo, DeltaRecord *undo) {
  TupleSlot result;
  accessor_.Allocate(insertion_head_, result);

  StorageUtil::WriteBytes(sizeof(DeltaRecord *), 0, accessor_.AccessForceNotNull(result, VERSION_VECTOR_COLUMN_ID));
  Update(result, redo, undo);
  return result;
}

DeltaRecord *DataTable::ReadVersionPtr(const TupleSlot &slot) {
  auto *ptr = accessor_.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  return reinterpret_cast<DeltaRecord *>(ptr);
}

} // namespace noisepage::storage
