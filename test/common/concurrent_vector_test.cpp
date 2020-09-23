#include "common/concurrent_vector.h"
#include <vector>
#include "gtest/gtest.h"

namespace noisepage {
class ConcurrentVectorTests : public ::testing::Test {};

TEST_F(ConcurrentVectorTests, SimpleTest) {
  ConcurrentVector<int> concurrent_vector;
  std::vector<int> stl_vector;
  
  const uint32_t num_inserts = 100;
  for (uint32_t i = 0; i < num_inserts; i++) {
    concurrent_vector.PushBack(i);
    stl_vector.push_back(i);
  }

  for (uint32_t i = 0; i < stl_vector.size(); i++) {
    EXPECT_EQ(concurrent_vector[i], stl_vector[i]);
  }

  uint32_t count = 0;
  for (auto it = concurrent_vector.Begin(); it != concurrent_vector.End(); ++it) {
    count++;
  }
  EXPECT_EQ(count, num_inserts);
}
}
