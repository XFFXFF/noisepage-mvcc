#include "storage/data_table.h"
#include "storage/storage_util.h"

#define VERSION_VECTOR_COLUMN_ID 0

namespace noisepage::storage {

DataTable::DataTable(BlockStore &store, BlockLayout layout)
    : block_store_(store), accessor_(layout) {
  NewBlock();
}

void DataTable::Select(timestamp_t timestamp, const TupleSlot &slot,
                       ProjectedRow *out_buffer) {
  for (uint16_t i = 0; i < out_buffer->NumColumns(); i++) {
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, out_buffer, i);
  }

  DeltaRecord *version_ptr = ReadVersionPtr(slot);
  if (version_ptr == nullptr) {
    return;
  }

  std::unordered_map<uint16_t, uint16_t> id_to_offset;
  for (auto i = 0; i < out_buffer->NumColumns(); i++) {
    uint16_t col_id = out_buffer->ColumnIds()[i];
    id_to_offset[col_id] = i;
  }

  while (version_ptr != nullptr && version_ptr->timestamp_ > timestamp) {
    StorageUtil::ApplyDelta(accessor_.GetBlockLayout(), *version_ptr->Delta(),
                            out_buffer, id_to_offset);
    version_ptr = version_ptr->next_;
  }
}

bool DataTable::Update(const TupleSlot &slot, const ProjectedRow &redo,
                       DeltaRecord *undo) {
  assert(redo.NumColumns() == undo->Delta()->NumColumns());

  DeltaRecord *version_ptr = ReadVersionPtr(slot);

  undo->next_ = version_ptr;

  if (HasConflict(version_ptr, undo))
    return false;

  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    StorageUtil::CopyAttrIntoProjection(accessor_, slot, undo->Delta(), i);
  }

  auto *ptr = accessor_.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  *reinterpret_cast<DeltaRecord **>(ptr) = undo;

  for (uint16_t i = 0; i < redo.NumColumns(); i++) {
    StorageUtil::CopyAttrFromProjection(redo, accessor_, slot, i);
  }
  return true;
}

TupleSlot DataTable::Insert(const ProjectedRow &redo, DeltaRecord *undo) {
  TupleSlot result;
  while (!accessor_.Allocate(insertion_head_, result)) {
    NewBlock();
  }

  StorageUtil::WriteBytes(
      sizeof(DeltaRecord *), 0,
      accessor_.AccessForceNotNull(result, VERSION_VECTOR_COLUMN_ID));
  Update(result, redo, undo);
  return result;
}

DeltaRecord *DataTable::ReadVersionPtr(const TupleSlot &slot) {
  auto *ptr = accessor_.AccessWithNullCheck(slot, VERSION_VECTOR_COLUMN_ID);
  return *reinterpret_cast<DeltaRecord **>(ptr);
}

} // namespace noisepage::storage
