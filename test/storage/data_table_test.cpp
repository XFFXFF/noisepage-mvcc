#include "storage/data_table.h"
#include "storage/storage_test_util.h"
#include "gtest/gtest.h"
#include <random>

namespace noisepage {
  
class DataTableTests : public ::testing::Test {};

TEST_F(DataTableTests, SimpleTest) {
  const uint32_t max_col = 100;
  std::default_random_engine generator;
  const uint32_t repeat = 2;

  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockStore block_store(10);
    storage::BlockLayout layout = testutil::RandomLayout(generator, max_col); 
    storage::DataTable data_table(block_store, layout);

    auto all_col_ids = testutil::ProjectionListAllColumns(layout);
    uint32_t redo_size = storage::ProjectedRow::Size(layout, all_col_ids);
    byte *redo_buffer = new byte[redo_size];
    memset(redo_buffer, 0, redo_size);
    auto *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, layout, all_col_ids);
    testutil::PopulateRandomRow(redo, layout, generator);
    storage::TupleSlot slot = data_table.Insert(*redo);

    byte *select_buffer = new byte[redo_size];
    memset(select_buffer, 0, redo_size);
    auto *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer, layout, all_col_ids);

    for (uint16_t i = 0; i < select_row->NumColumns(); i++) {
      EXPECT_FALSE(select_row->AccessWithNullCheck(i) != nullptr);
    }

    data_table.Select(slot, select_row);

    for (uint16_t i = 0; i < select_row->NumColumns(); i++) {
      EXPECT_TRUE(select_row->AccessWithNullCheck(i) != nullptr);
    }

    delete redo_buffer;
    delete select_buffer;
  }
}
} // namespace noisepage
