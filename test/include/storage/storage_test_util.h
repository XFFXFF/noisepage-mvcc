#pragma once
#include <vector>
#include "storage/storage_defs.h"
#include "common/test_util.h"

namespace noisepage
{
namespace testutil
{
template<typename Random>
storage::BlockLayout RandomLayout(Random &generator, uint16_t max_col=UINT16_MAX) {
  uint16_t num_attrs = std::uniform_int_distribution<>(1, max_col)(generator);
  std::vector<uint8_t> possible_attr_sizes = {1, 2, 4, 8}; 
  std::vector<uint8_t> attr_sizes(num_attrs);
  for (auto i = 0; i < num_attrs; i++) {
    auto it = UniformRandomElement(possible_attr_sizes, generator);
    attr_sizes[i] = *it;
  }
  return {num_attrs, attr_sizes};
}  
 
} // namespace testutil

  
} // namespace noisepage
