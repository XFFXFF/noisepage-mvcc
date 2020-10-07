#include "storage/data_table.h"
#include "storage/storage_test_util.h"
#include "gtest/gtest.h"
#include <random>

namespace noisepage {
class RandomDataTableTestObject {
public:
  template<typename Random>
  RandomDataTableTestObject(storage::BlockStore &store, uint32_t max_col, double null_bias, Random &generator) 
                            : layout_(testutil::RandomLayout(generator, max_col)), 
                              data_table_(store, layout_), null_bias_(null_bias) {}
  
  ~RandomDataTableTestObject() {
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
    
    tuple_versions_[slot].emplace_back(0, redo);
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
    if (result) {
      byte *version_buffer = new byte[redo_size_];
      loose_pointers_.push_back(version_buffer);
      memset(version_buffer, 0, redo_size_);
      memcpy(version_buffer, tuple_versions_[slot].back().second, redo_size_);
      auto *version = reinterpret_cast<storage::ProjectedRow *>(version_buffer);

      std::unordered_map<uint16_t, uint16_t> id_to_offset;
      for (auto i = 0; i < version->NumColumns(); i++) {
        id_to_offset[version->ColumnIds()[i]] = i;
      }

      storage::StorageUtil::ApplyDelta(layout_, *update, version, id_to_offset);
      tuple_versions_[slot].emplace_back(timestamp, version);
    }
    delete[] update_buffer;
    return result;
  }

  storage::ProjectedRow *SelectIntoBuffer(const storage::TupleSlot &slot, timestamp_t timestamp) {
    memset(select_buffer_, 0, redo_size_);
    auto *select_row = storage::ProjectedRow::InitializeProjectedRow(select_buffer_, layout_, all_col_ids_);
    data_table_.Select(timestamp, slot, select_row);
    return select_row;
  }

  storage::ProjectedRow *GetInsertedRow(const storage::TupleSlot &slot, timestamp_t timestamp) {
    assert(tuple_versions_.find(slot) != tuple_versions_.end());
    auto &versions = tuple_versions_[slot];
    for (auto i = static_cast<int64_t>(versions.size()-1); i >= 0; i--) {
      if (timestamp >= versions[i].first) {
        return versions[i].second;
      }
    }
    return nullptr;
  }

  const storage::BlockLayout &Layout() const {
    return layout_;
  }
private:
  const storage::BlockLayout layout_;
  storage::DataTable data_table_;
  const double null_bias_;
  std::vector<byte *> loose_pointers_;
  using tuple_version = std::pair<timestamp_t, storage::ProjectedRow *>;
  std::unordered_map<storage::TupleSlot, std::vector<tuple_version>> tuple_versions_;

  std::vector<uint16_t> all_col_ids_{testutil::ProjectionListAllColumns(layout_)};
  uint32_t redo_size_ = storage::ProjectedRow::Size(layout_, all_col_ids_);
  uint32_t undo_size_ = storage::DeltaRecord::Size(layout_, all_col_ids_);
  byte *select_buffer_ = new byte[redo_size_];
};
  
struct DataTableTests : public ::testing::Test {
  storage::BlockStore block_store_{10};
  std::default_random_engine generator_;
  std::uniform_real_distribution<double> null_ratio_{0.0, 1.0};
};

TEST_F(DataTableTests, SimpleInsertSelect) {
  const uint32_t repeat = 10;
  const uint32_t max_col = 100;

  for (uint32_t i = 0; i < repeat; i++) {
    RandomDataTableTestObject tested(block_store_, max_col, null_ratio_(generator_), generator_);
    std::vector<storage::TupleSlot> inserted_slots_;
    
    for (uint32_t j = 0; j < 10; j++) {
      auto slot = tested.InsertRandomTuple(generator_);
      inserted_slots_.push_back(slot);
    }

    for (auto slot : inserted_slots_) {
      auto *select_row = tested.SelectIntoBuffer(slot, 0);
      EXPECT_TRUE(testutil::ProjectionListEqual(tested.Layout(), *select_row, 
                                                *tested.GetInsertedRow(slot, 0)));
    }
  }
}

TEST_F(DataTableTests, SimpleVersionChain) {
  const uint32_t repeat = 1;
  const uint32_t max_col = 100;

  for (uint32_t i = 0; i < repeat; i++) {
    RandomDataTableTestObject tested(block_store_, max_col, null_ratio_(generator_), generator_);

    auto slot = tested.InsertRandomTuple(generator_);

    EXPECT_TRUE(tested.RandomUpdateTuple(timestamp_t(0), slot, generator_));

    auto *select_row = tested.SelectIntoBuffer(slot, 0);
    EXPECT_TRUE(testutil::ProjectionListEqual(tested.Layout(), *select_row,
                                              *tested.GetInsertedRow(slot, 0)));
  }
}

} // namespace noisepage
