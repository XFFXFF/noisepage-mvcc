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
  uint32_t redo_size = storage::ProjectedRow::Size(layout);
  auto *redu_buffer = new byte[redo_size];
  storage::ProjectedRow::InitializeProjectedRow(redu_buffer, layout);
}
}