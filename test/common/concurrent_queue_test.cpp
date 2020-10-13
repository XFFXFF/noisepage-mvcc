#include "common/concurrent_queue.h"
#include "gtest/gtest.h"
#include <iostream>

namespace noisepage {
class ConcurrentQueueTests : public ::testing::Test {};

TEST_F(ConcurrentQueueTests, SimpleTest) {
  const int max_queue_size = 100;
  ConcurrentQueue<int> queue;
  for (int i = 0; i < max_queue_size; i++) {
    queue.Enqueue(std::move(i));
  }
  EXPECT_EQ(queue.UnsafeSize(), max_queue_size);
  int dest;
  for (int i = 0; i < max_queue_size; i++) {
    queue.Dequeue(dest);
    EXPECT_EQ(dest, i);
  }
  EXPECT_EQ(queue.UnsafeSize(), 0);
}

} // namespace noisepage
