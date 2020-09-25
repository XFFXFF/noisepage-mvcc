#include "storage/data_table.h"

namespace noisepage::storage {

DataTable::DataTable(BlockStore store, BlockLayout layout) : block_store_(store), accessor_(layout) {
  insertion_head_ = block_store_.Get();
} 

} // namespace noisepage::storage
