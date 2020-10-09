#include "storage/data_table.h"
#include "storage/storage_test_util.h"
#include "gtest/gtest.h"
#include <random>

namespace noisepage {
class FakeTransaction {
public:
  template<typename Random>
  FakeTransaction(storage::BlockStore &store, uint32_t max_col, double null_bias,
                  timestamp_t start_time, timestamp_t txn_id, Random &generator) 
                  : layout_(testutil::RandomLayout(generator, max_col)), 
                    data_table_(store, layout_), null_bias_(null_bias), 
                    start_time_(start_time), txn_id_(txn_id) {}
  
  ~FakeTransaction() {
    for (auto *ptr : loose_pointers_) {
      delete[] ptr;
    }
    delete[] select_buffer_;
  }

  template<typename Random>
  storage::TupleSlot InsertRandomTuple(Random generator) {
    byte *redo_buffer = new byte[redo_size_];
    loose_pointers_.push_back(redo_buffer);
    memset(redo_buffer, 0, redo_size_);
    auto *redo = storage::ProjectedRow::InitializeProjectedRow(redo_buffer, layout_, all_col_ids_);
    testutil::PopulateRandomRow(redo, layout_, null_bias_, generator);

    byte *undo_buffer = new byte[undo_size_];
    loose_pointers_.push_back(undo_buffer);
    memset(undo_buffer, 0, undo_size_);
    auto *undo = storage::DeltaRecord::InitializeDeltaRecord(undo_buffer, 0, layout_, all_col_ids_);
    storage::TupleSlot slot = data_table_.Insert(*redo, undo);
    
    reference_tuples.emplace(slot, redo);
    return slot;
  }

  template<typename Random>
  bool RandomUpdateTuple(const timestamp_t timestamp, const storage::TupleSlot &slot, Random &generator) {
    std::vector<uint16_t> update_col_ids = testutil::ProjectionListRandomColumns(layout_, generator);
    uint32_t update_size = storage::ProjectedRow::Size(layout_, update_col_ids);
    byte *update_buffer = new byte[update_size];
    memset(update_buffer, 0, update_size);
    auto *update = storage::ProjectedRow::InitializeProjectedRow(update_buffer, layout_, update_col_ids);
    testutil::PopulateRandomRow(update, layout_, null_bias_, generator);

    byte *undo_buffer = new byte[storage::DeltaRecord::Size(layout_, update_col_ids)];
    loose_pointers_.push_back(undo_buffer);
    memset(undo_buffer, 0, update_size);
    auto *undo = storage::DeltaRecord::InitializeDeltaRecord(undo_buffer, timestamp, layout_, update_col_ids);

    bool result = data_table_.Update(slot, *update, undo);
    delete[] update_buffer;
    return result;
  }

  storage::ProjectedRow *SelectIntoBuffer(const storage::TupleSlot &slot, timestamp_t timestamp) {
    memset(select_buffer_, 0, redo_size_);
    auto *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer_, layout_, all_col_ids_);
    data_table_.Select(timestamp, slot, select_row);
    return select_row;
  }

  storage::ProjectedRow *GetInsertedRow(const storage::TupleSlot &slot) {
    assert(reference_tuples.find(slot) != reference_tuples.end());
    return reference_tuples[slot];
  }

  const storage::BlockLayout &Layout() const {
    return layout_;
  }
private:
  const storage::BlockLayout layout_;
  storage::DataTable data_table_;
  const double null_bias_;
  timestamp_t start_time_, txn_id_;

  std::vector<byte *> loose_pointers_;
  std::unordered_map<storage::TupleSlot, storage::ProjectedRow *> reference_tuples;

  std::vector<uint16_t> all_col_ids_{testutil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  uint32_t undo_size_ = storage::DeltaRecord::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};
  
struct DataTableConcurrentTests : public ::testing::Test {
  storage::BlockStore block_store_{10};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

TEST_F(DataTableConcurrentTests, SimpleInsertSelect) {
  const uint32_t repeat = 10;
  const uint32_t max_col = 100;

  for (uint32_t i = 0; i < repeat; i++) {
    FakeTransaction tested(block_store_, max_col, null_ratio_(generator_),
                           timestamp_t(0), timestamp_t(0), generator_);
    std::vector<storage::TupleSlot> inserted_slots_;
    
    for (uint32_t j = 0; j < 10; j++) {
      auto slot = tested.InsertRandomTuple(generator_);
      inserted_slots_.push_back(slot);
    }

    for (auto slot : inserted_slots_) {
      auto *select_row = tested.SelectIntoBuffer(slot, 0);
      EXPECT_TRUE(testutil::ProjectionListEqual(tested.Layout(), *select_row, 
                                                *tested.GetInsertedRow(slot)));
    }
  }
}
}