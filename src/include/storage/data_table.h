#pragma once
#include "common/concurrent_vector.h"
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace noisepage::storage {
class DataTable {
public:
  DataTable(BlockStore &store, BlockLayout layout);
  ~DataTable() {
    for (auto it = blocks_.Begin(); it != blocks_.End(); ++it) {
      block_store_.Release(*it);
    }
  }

  void Select(const TupleSlot &slot, ProjectedRow *out_buffer);

  bool Update(const TupleSlot &slot, const ProjectedRow &redo,
              DeltaRecord *undo);

  TupleSlot Insert(const ProjectedRow &redo, DeltaRecord *undo);

private:
  BlockStore &block_store_;
  TupleAccessStrategy accessor_;
  RawBlock *insertion_head_;
  ConcurrentVector<RawBlock *> blocks_;

  DeltaRecord *ReadVersionPtr(const TupleSlot &slot);
};

} // namespace noisepage::storage
