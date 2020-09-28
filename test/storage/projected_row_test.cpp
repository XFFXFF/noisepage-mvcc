#include <random>
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "storage/storage_test_util.h"

namespace noisepage {

class ProjectedRowTests : public ::testing::Test {};

TEST_F(ProjectedRowTests, SimpleTest) {
  std::default_random_engine generator;
  std::uniform_real_distribution<double> null_ratio{0.0, 1.0};
  const uint32_t max_col = 100;
  const uint32_t repeat = 10;
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator, max_col);
    std::vector<uint16_t> col_ids; 
    for (uint16_t i = 1; i < layout.num_cols_; i++) {
      col_ids.push_back(i);
    }
    uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
    auto *redo_buffer = new byte[redo_size];
    memset(redo_buffer, 0, redo_size);
    storage::ProjectedRow *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, layout, col_ids);
    for (uint32_t i = 0; i < redo->NumColumns(); i++) {
      EXPECT_FALSE(redo->NullBitmap()->Test(i));
    }

    testutil::PopulateRandomRow(redo, layout, null_ratio(generator), generator);
    // testutil::PrintRow(redo, layout); 
  }
}
}
