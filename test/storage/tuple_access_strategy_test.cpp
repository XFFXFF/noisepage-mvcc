#include "gtest/gtest.h"
#include "storage/tuple_access_strategy.h"
#include "common/test_util.h"

namespace noisepage
{
class TupleAccessStrategyTests : public ::testing::Test {};

TEST_F(TupleAccessStrategyTests, NullTest) {
  std::default_random_engine generator;

  storage::RawBlock *raw_block = new storage::RawBlock();
  memset(raw_block, 0, sizeof(storage::RawBlock));

  const uint32_t repeat = 1;
  for (auto i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    storage::TupleAccessStrategy tested(layout);
    storage::InitializeRawBlock(raw_block, layout, 0);

    auto *block = reinterpret_cast<storage::Block *>(raw_block);

    uint32_t offset;
    EXPECT_TRUE(tested.Allocate(block, offset));
  }
}
  
} // namespace noisepage
