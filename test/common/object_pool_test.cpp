#include "common/object_pool.h"
#include "gtest/gtest.h"

namespace noisepage
{
class ObjectPoolTests : public ::testing::Test {};

TEST_F(ObjectPoolTests, SimpleTest) {
  const uint32_t reuse_limit = 1;
  const uint32_t repeat = 10;
  ObjectPool<int> tested(reuse_limit);

  int *reuse_ptr = tested.Get();
  tested.Release(reuse_ptr);

  for (uint32_t i = 0; i < repeat; i++) {
    EXPECT_EQ(tested.Get(), reuse_ptr);
    tested.Release(reuse_ptr);
  }
}
} // namespace noisepage


