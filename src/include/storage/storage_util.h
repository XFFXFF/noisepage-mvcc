#pragma once
#include "storage/tuple_access_strategy.h"
#include <iostream>
#include <unordered_map>

namespace noisepage::storage {
class StorageUtil {
public:
  StorageUtil() = delete;
  static uint64_t ReadBytes(uint8_t size, const byte *pos) {
    switch (size) {
    case 1:
      return *reinterpret_cast<const uint8_t *>(pos);
    case 2:
      return *reinterpret_cast<const uint16_t *>(pos);
    case 4:
      return *reinterpret_cast<const uint32_t *>(pos);
    case 8:
      return *reinterpret_cast<const uint64_t *>(pos);
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

  static void CopyAttrIntoProjection(const TupleAccessStrategy &accessor,
                                     const TupleSlot &slot, ProjectedRow *to,
                                     uint16_t projection_list_offset) {
    uint16_t col_id = to->ColumnIds()[projection_list_offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
    auto *store_attr = accessor.AccessWithNullCheck(slot, col_id);

    if (store_attr == nullptr) {
      to->SetNull(projection_list_offset);
    } else {
      auto *dest = to->AccessForceNotNull(projection_list_offset);
      WriteBytes(attr_size, ReadBytes(attr_size, store_attr), dest);
    }
  }

  static void CopyAttrFromProjection(const ProjectedRow &from,
                                     TupleAccessStrategy &accessor,
                                     const TupleSlot &slot,
                                     uint16_t projection_list_offset) {
    const byte *store_attr = from.AccessWithNullCheck(projection_list_offset);
    uint16_t col_id = from.ColumnIds()[projection_list_offset];
    uint8_t attr_size = accessor.GetBlockLayout().attr_sizes_[col_id];
    if (store_attr == nullptr) {
      accessor.SetNull(slot, col_id);
    } else {
      auto *dest = accessor.AccessForceNotNull(slot, col_id);
      WriteBytes(attr_size, ReadBytes(attr_size, store_attr), dest);
    }
  }

  static void ApplyDelta(const BlockLayout &layout, const ProjectedRow &delta, 
                         ProjectedRow *buffer, const std::unordered_map<uint16_t, uint16_t> &id_to_offset) {
    for (uint16_t i = 0; i < delta.NumColumns(); i++) {
      const byte *delta_attr = delta.AccessWithNullCheck(i);
      uint16_t col_id = delta.ColumnIds()[i];
      auto it = id_to_offset.find(col_id);
      assert(it != id_to_offset.end());
      if (delta_attr == nullptr) {
        buffer->SetNull(it->second);
      } else {
        uint8_t attr_size = layout.attr_sizes_[col_id];
        auto *dest = buffer->AccessForceNotNull(it->second);
        WriteBytes(attr_size, ReadBytes(attr_size, delta_attr), dest);
      }
    }
  }
};
} // namespace noisepage::storage
