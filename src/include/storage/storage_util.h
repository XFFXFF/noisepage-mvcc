#pragma once
#include "storage/tuple_access_strategy.h"
#include <iostream>

namespace noisepage::storage {
class StorageUtil {
public:
  StorageUtil() = delete;
  static uint64_t ReadBytes(uint8_t size, byte *pos) {
    switch (size) {
    case 1:
      return *reinterpret_cast<uint8_t *>(pos);
    case 2:
      return *reinterpret_cast<uint16_t *>(pos);
    case 4:
      return *reinterpret_cast<uint32_t *>(pos);
    case 8:
      return *reinterpret_cast<uint64_t *>(pos);
    default:
      throw std::runtime_error("Invalid byte read value");
    }
  }

  static void WriteBytes(uint8_t size, uint64_t val, byte *pos) {
    switch (size) {
    case 1:
      *reinterpret_cast<uint8_t *>(pos) = static_cast<uint8_t>(val);
      break;
    case 2:
      *reinterpret_cast<uint16_t *>(pos) = static_cast<uint16_t>(val);
      break;
    case 4:
      *reinterpret_cast<uint32_t *>(pos) = static_cast<uint32_t>(val);
      break;
    case 8:
      *reinterpret_cast<uint64_t *>(pos) = static_cast<uint64_t>(val);
      break;
    default:
      throw std::runtime_error("Invalid byte write value");
    }
  }

  static void CopyAttrIntoProjection(TupleAccessStrategy &accessor,
                                     TupleSlot &slot, 
                                     ProjectedRow *to,
                                     uint16_t projection_list_offset) {
    uint16_t col_id = to->ColumnIds()[projection_list_offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
    auto *store_attr = accessor.AccessWithNullCheck(slot.GetBlock(), col_id, slot.GetOffset());
    
    if (store_attr == nullptr) {
      to->SetNull(projection_list_offset);
    } else {
      auto *dest = to->AccessForceNotNull(projection_list_offset);
      WriteBytes(attr_size, ReadBytes(attr_size, store_attr), dest);
    }
  }

  static void CopyAttrFromProjection(ProjectedRow *from,
                                     TupleAccessStrategy &accessor,
                                     TupleSlot &slot,
                                     uint16_t projection_list_offset) {
    auto *store_attr = from->AccessWithNullCheck(projection_list_offset);
    uint16_t col_id = from->ColumnIds()[projection_list_offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
    if (store_attr == nullptr) {
      accessor.SetNull(slot.GetBlock(), col_id, slot.GetOffset());
    } else {
      auto *dest = accessor.AccessForceNotNull(slot.GetBlock(), col_id, slot.GetOffset());
      WriteBytes(attr_size, ReadBytes(attr_size, store_attr), dest);
    }
  }
};
} // namespace noisepage::storage
