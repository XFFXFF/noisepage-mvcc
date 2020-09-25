#pragma once
#include "storage/storage_defs.h"
#include "storage/tuple_access_strategy.h"

namespace noisepage::storage {
class DataTable {
public:
  DataTable(BlockStore store, BlockLayout layout);
private:
  BlockStore block_store_;
  TupleAccessStrategy accessor_;
  RawBlock *insertion_head_;
};
  
} // namespace noisepage::storage
