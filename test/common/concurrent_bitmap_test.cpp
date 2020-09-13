#include "common/concurrent_bitmap.h"
#include "gtest/gtest.h"
#include <bitset>
#include <random>
#include <functional>
#include <thread>

namespace noisepage
{
template<uint32_t N>
void CheckReferenceBitmap(const ConcurrentBitmap<N> &bitmap, const std::bitset<N> &stl_bitmap) {
  for (uint32_t i = 0; i < N; i++) {
    EXPECT_EQ(bitmap[i], stl_bitmap[i]);
  }
}

void RunThreadUntilFinish(uint32_t num_threads,
                          const std::function<void(uint32_t)> &workload) {
  std::vector<std::thread> threads;
  for (auto i = 0; i < num_threads; i++) {
    threads.emplace_back([i, &workload] { workload(i); });
  }
  for (auto &thread : threads) {
    thread.join();
  }
} 

class ConcurrentBitmapTests : public ::testing::Test {}; 

TEST_F(ConcurrentBitmapTests, SimpleTest) {
  const uint32_t num_elements = 100;
  ConcurrentBitmap<num_elements> tested;

  for (uint32_t i = 0; i < num_elements; i++) {
    EXPECT_FALSE(tested.Test(i));
  } 

  std::bitset<num_elements> stl_bitmap;
  CheckReferenceBitmap<num_elements>(tested, stl_bitmap);

  uint32_t num_iterations = 3;
  std::default_random_engine generator;
  std::uniform_int_distribution<> pos_dist(0, num_elements - 1);
  for (auto i = 0; i < num_iterations; i++) {
    auto pos = pos_dist(generator);
    EXPECT_TRUE(tested.Flip(pos, tested.Test(pos)));
    stl_bitmap.flip(pos);
    CheckReferenceBitmap<num_elements>(tested, stl_bitmap);
  }

  auto pos = pos_dist(generator);
  EXPECT_FALSE(tested.Flip(pos, !tested.Test(pos)));
}

TEST_F(ConcurrentBitmapTests, ConcurrentCorrectnessTest) {
  const uint32_t num_elements = 10000;
  const uint32_t num_threads = 8;
  ConcurrentBitmap<num_elements> tested;

  std::vector<std::vector<uint32_t>> elements(num_elements);

  auto workload = [&](uint32_t thread_id) {
    for (auto i = 0; i < num_elements; i++) {
      if (tested.Flip(i, false)) {
        elements[thread_id].push_back(i);
      }
    }
  };

  RunThreadUntilFinish(num_threads, workload);

  std::vector<uint32_t> all_elements;
  for (auto i = 0; i < num_threads; i++) {
    all_elements.insert(all_elements.end(), 
                        elements[i].begin(),
                        elements[i].end());
  }

  EXPECT_EQ(all_elements.size(), num_elements);
  // std::sort(all_elements.begin(), all_elements.end());
  // for (uint32_t i = 0; i < num_elements; i++) {
  //   EXPECT_EQ(all_elements[i], i);
  // }
}

}