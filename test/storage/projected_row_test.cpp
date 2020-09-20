#include <random>
#include "gtest/gtest.h"
#include "storage/storage_defs.h"
#include "storage/storage_test_util.h"

namespace noisepage {

class ProjectedRowTests : public ::testing::Test {};

TEST_F(ProjectedRowTests, SimpleTest) {
  std::default_random_engine generator;
  const uint32_t max_col = 100;
  storage::BlockLayout layout = testutil::RandomLayout(generator, max_col);
  std::vector<uint16_t> col_ids; 
  for (uint16_t i = 1; i < layout.num_cols_; i++) {
    col_ids.push_back(i);
  }
  uint32_t redo_size = storage::ProjectedRow::Size(layout, col_ids);
  auto *redu_buffer = new byte[redo_size];
  storage::ProjectedRow::InitializeProjectedRow(redu_buffer, layout, col_ids);
}
}