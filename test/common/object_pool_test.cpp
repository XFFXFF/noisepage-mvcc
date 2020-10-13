#include "common/object_pool.h"
#include "gtest/gtest.h"
#include <atomic>
#include <random>
#include <thread>

namespace noisepage {
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

class ObjectPoolTestType {
public:
  ObjectPoolTestType *Use() {
    bool expected = false;
    EXPECT_TRUE(in_use_.compare_exchange_strong(expected, true));
    return this;
  }

  ObjectPoolTestType *Release() {
    in_use_.store(false);
    return this;
  }

private:
  std::atomic<bool> in_use_;
};

// 当多个线程对同一个object_pool调用Get方法时，不希望object_pool把
// 同一内存分配给不同的线程
TEST_F(ObjectPoolTests, ConcurrentCorrectnessTest) {
  const uint32_t reuse_limit = 100;
  ObjectPool<ObjectPoolTestType> tested(reuse_limit);

  auto workload = [&] {
    std::default_random_engine generator;
    const double allocate_ratio = 0.5;
    std::bernoulli_distribution op_dist(allocate_ratio);

    std::vector<ObjectPoolTestType *> ptrs;

    const uint32_t trial = 100;
    for (uint32_t i = 0; i < trial; i++) {
      if (op_dist(generator)) {
        ptrs.push_back(tested.Get()->Use());
      } else if (!ptrs.empty()) {
        std::uniform_int_distribution<> pos_dist(0, (int)ptrs.size() - 1);
        int pos = pos_dist(generator);
        tested.Release(ptrs[pos]->Release());
        ptrs.erase(ptrs.begin() + pos);
      }
    }

    for (auto ptr : ptrs) {
      tested.Release(ptr->Release());
    }
  };

  const uint32_t repeat = 100;
  for (uint32_t i = 0; i < repeat; i++) {
    const uint32_t threads_num = 8;
    std::vector<std::thread> threads;
    for (uint32_t j = 0; j < threads_num; j++) {
      threads.emplace_back(workload);
    }
    for (auto &thread : threads) {
      thread.join();
    }
  }
}
} // namespace noisepage
