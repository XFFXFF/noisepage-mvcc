#include "gtest/gtest.h"
#include "storage/tuple_access_strategy.h"
#include "common/test_util.h"

namespace noisepage
{
class TupleAccessStrategyTests : public ::testing::Test {};

TEST_F(TupleAccessStrategyTests, NullTest) {
  std::default_random_engine generator;

  storage::RawBlock *raw_block = new storage::RawBlock();

  const uint32_t repeat = 5;
  for (auto i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    storage::TupleAccessStrategy tested(layout);
    memset(raw_block, 0, sizeof(storage::RawBlock));
    storage::InitializeRawBlock(raw_block, layout, 0);

    auto *block = reinterpret_cast<storage::Block *>(raw_block);

    uint32_t offset;
    EXPECT_TRUE(tested.Allocate(block, offset));

    EXPECT_TRUE(tested.AccessWithNullCheck(block, 0, offset) != nullptr);
    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      EXPECT_FALSE(tested.AccessWithNullCheck(block, j, offset) != nullptr);
    }

    std::vector<bool> nulls(layout.num_cols_);
    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      if (std::bernoulli_distribution(0.5)(generator)) {
        tested.AccessForceNotNull(block, j, offset);
        nulls[j] = false;
      } else {
        nulls[j] = true;
      }
    }

    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      if (nulls[j]) {
        EXPECT_FALSE(tested.AccessWithNullCheck(block, j, offset) != nullptr);
      } else {
        EXPECT_TRUE(tested.AccessWithNullCheck(block, j, offset) != nullptr);
      }
    }

  }
}
  
} // namespace noisepage
