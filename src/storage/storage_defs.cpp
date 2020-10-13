#include "storage/storage_defs.h"

namespace noisepage::storage {

uint32_t ProjectedRow::Size(const BlockLayout &layout,
                            const std::vector<uint16_t> &col_ids) {
  uint32_t row_size = sizeof(uint16_t);
  for (auto col_id : col_ids) {
    row_size += (sizeof(uint16_t) + sizeof(uint32_t));
    row_size += layout.attr_sizes_[col_id];
  }
  row_size += BitmapSize(layout.num_cols_ - 1);
  return row_size;
}

ProjectedRow *
ProjectedRow::InitializeProjectedRow(byte *head, const BlockLayout &layout,
                                     const std::vector<uint16_t> &col_ids) {
  auto *row = reinterpret_cast<ProjectedRow *>(head);
  row->NumColumns() = static_cast<uint16_t>(col_ids.size());

  uint32_t val_offset = sizeof(uint16_t) +
                        row->num_cols_ * (sizeof(uint32_t) + sizeof(uint16_t)) +
                        BitmapSize(row->num_cols_);
  for (uint16_t i = 0; i < row->num_cols_; i++) {
    row->ColumnIds()[i] = col_ids[i];
    row->AttrValueOffset()[i] = val_offset;
    val_offset += layout.attr_sizes_[col_ids[i]];
  }
  return row;
}
} // namespace noisepage::storage