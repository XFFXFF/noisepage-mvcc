#pragma once
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"
#include "common/concurrent_vector.h"

namespace noisepage::storage {
class DataTable {
public:
  DataTable(BlockStore store, BlockLayout layout);
  ~DataTable() {
    for (auto it = blocks_.Begin(); it != blocks_.End(); ++it) {
      block_store_.Release(*it);
    }
  }

  void Select(TupleSlot &slot, ProjectedRow *out_buffer);

  TupleSlot Insert(const ProjectedRow &redo);
private:
  BlockStore block_store_;
  TupleAccessStrategy accessor_;
  RawBlock *insertion_head_;
  ConcurrentVector<RawBlock *> blocks_;
};
  
} // namespace noisepage::storage
