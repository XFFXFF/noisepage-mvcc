#include "common/concurrent_map.h"
#include "gtest/gtest.h"

namespace noisepage
{
class ConcurrentMapTests : public ::testing::Test {};

TEST_F(ConcurrentMapTests, SimpleTest) {
  ConcurrentMap<uint32_t, uint32_t> tested;
  const uint32_t max_insert = 100;
  const uint32_t max_erase = 50;

  for (uint32_t i = 0; i < max_insert; i++) {
    EXPECT_TRUE(tested.Insert(i, i));
  }

  for (uint32_t i = 0; i < max_erase; i++) {
    tested.UnsafeErase(i);
  }

  for (uint32_t i = max_erase; i < max_insert; i++) {
    uint32_t val;
    EXPECT_TRUE(tested.Find(i, val));
    EXPECT_EQ(val, i);
  }
}
  
} // namespace noisepage
