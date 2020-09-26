#include <unordered_map>
#include "gtest/gtest.h"
#include "storage/tuple_access_strategy.h"
#include "storage/tuple_access_strategy_test_util.h"
#include "storage/storage_test_util.h"

namespace noisepage
{
struct TupleAccessStrategyTests : public ::testing::Test {
  storage::RawBlock *raw_block_ = nullptr;
  protected:
    void SetUp() override {
      raw_block_ = new storage::RawBlock();
    }

    void TearDown() override {
      delete raw_block_;
    }
};

TEST_F(TupleAccessStrategyTests, NullTest) {
  std::default_random_engine generator;

  const uint32_t repeat = 5;
  for (auto i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator);
    storage::TupleAccessStrategy tested(layout);
    memset(raw_block_, 0, sizeof(storage::RawBlock));
    storage::InitializeRawBlock(raw_block_, layout, 0);

    storage::TupleSlot slot;
    EXPECT_TRUE(tested.Allocate(raw_block_, slot));

    EXPECT_TRUE(tested.AccessWithNullCheck(slot, 0) != nullptr);
    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      EXPECT_FALSE(tested.AccessWithNullCheck(slot, j) != nullptr);
    }

    std::vector<bool> nulls(layout.num_cols_);
    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      if (std::bernoulli_distribution(0.5)(generator)) {
        tested.AccessForceNotNull(slot, j);
        nulls[j] = false;
      } else {
        nulls[j] = true;
      }
    }

    for (uint16_t j = 1; j < layout.num_cols_; j++) {
      if (nulls[j]) {
        EXPECT_FALSE(tested.AccessWithNullCheck(slot, j) != nullptr);
      } else {
        EXPECT_TRUE(tested.AccessWithNullCheck(slot, j) != nullptr);
      }
    }

  }
}

TEST_F(TupleAccessStrategyTests, SimpleInsertTest) {
  const uint32_t repeat = 5;
  const uint16_t max_col = 1000;
  std::default_random_engine generator;
  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator, max_col);
    storage::TupleAccessStrategy tested(layout);
    memset(raw_block_, 0, sizeof(storage::RawBlock));
    storage::InitializeRawBlock(raw_block_, layout, 0);

    std::unordered_map<uint32_t, testutil::FakeRawTuple> tuples;

    const uint32_t num_insert = 100; 
    
    for (uint32_t j = 0; j < num_insert; j++) {
      testutil::TryInsertFakeTuple(layout, tested, raw_block_, tuples, generator);
    }

    for (const auto &tuple : tuples) {
      auto offset = tuple.first;
      for (uint16_t col_id = 0; col_id < layout.num_cols_; col_id++) {
        auto val1 = tuple.second.Attribute(col_id);
        storage::TupleSlot slot(raw_block_, offset);
        byte *pos = tested.AccessWithNullCheck(slot, col_id);
        auto val2 = testutil::ReadByteValue(layout.attr_sizes_[col_id], pos);
        EXPECT_EQ(val1, val2);
      }
    }
  }
}

TEST_F(TupleAccessStrategyTests, ConcureentInsertTest) {
  std::default_random_engine generator;
  const uint32_t repeat = 100;
  const uint16_t max_col = 1000;
  const uint32_t num_thread = 8;

  for (uint32_t i = 0; i < repeat; i++) {
    storage::BlockLayout layout = testutil::RandomLayout(generator, max_col);
    storage::TupleAccessStrategy tested(layout);
    memset(raw_block_, 0, sizeof(storage::RawBlock));
    storage::InitializeRawBlock(raw_block_, layout, 0);

    std::vector<std::unordered_map<uint32_t, testutil::FakeRawTuple>> tuples(num_thread);

    auto workload = [&](uint32_t thread_id) {
      std::default_random_engine thread_generator(thread_id);
      uint32_t num_insert = layout.num_slots_ / num_thread ? layout.num_slots_ < 100000 : 1000;
      for (uint32_t j = 0; j < num_insert; j++) {
        testutil::TryInsertFakeTuple(layout, tested, raw_block_, tuples[thread_id], thread_generator);
      }
    };

    testutil::RunThreadUntilFinish(num_thread, workload);
    for (auto &thread_tuple : tuples) {
      for (auto &tuple : thread_tuple) {
        for (uint16_t col_id = 0; col_id < layout.num_cols_; col_id++) {
          auto val1 = tuple.second.Attribute(col_id);
          storage::TupleSlot slot(raw_block_, tuple.first);
          auto *pos = tested.AccessWithNullCheck(slot, col_id);
          auto val2 = testutil::ReadByteValue(layout.attr_sizes_[col_id], pos);
          EXPECT_EQ(val1, val2);
        }
      }
    }

  }
}
  
} // namespace noisepage
