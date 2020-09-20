#include "storage/storage_defs.h"

namespace noisepage::storage {

uint32_t ProjectedRow::Size( const BlockLayout &layout) {
  uint32_t row_size = sizeof(uint16_t);
  for (uint32_t i = 1; i < layout.num_cols_; i++) {
    row_size += (sizeof(uint32_t) + sizeof(uint16_t));
    row_size += layout.attr_sizes_[i];
  } 
  row_size += BitmapSize(layout.num_cols_ - 1);
  return row_size;
}

ProjectedRow *ProjectedRow::InitializeProjectedRow(byte *head, const BlockLayout &layout) {
  auto *row = reinterpret_cast<ProjectedRow *>(head);
  row->NumColumns() = layout.num_cols_ - 1;

  uint32_t val_offset = sizeof(uint16_t) + row->num_cols_ * (sizeof(uint32_t) + sizeof(uint16_t)) + BitmapSize(row->num_cols_);
  for (uint16_t i = 0; i < row->num_cols_; i++) {
    row->ColumnIds()[i] = i+1;
    row->AttrValueOffset()[i] = val_offset;
    val_offset += layout.attr_sizes_[i+1];
  }
  return row;
}
}