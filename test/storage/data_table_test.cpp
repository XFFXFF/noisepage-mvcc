#include "storage/data_table.h"
#include "storage/storage_test_util.h"
#include "gtest/gtest.h"
#include <random>

namespace noisepage {
  
class DataTableTests : public ::testing::Test {};

TEST_F(DataTableTests, SimpleTest) {
  const uint32_t max_col = 100;
  std::default_random_engine generator;
  storage::BlockStore block_store(10);
  storage::BlockLayout layout = testutil::RandomLayout(generator, max_col); 
  storage::DataTable data_table(block_store, layout);
}
} // namespace noisepage
