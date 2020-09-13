#pragma once

#include <random>
#include "storage/tuple_access_strategy.h"

namespace noisepage
{
namespace testutil
{
template<typename T, typename Random>
typename std::vector<T>::iterator UniformRandomElement(std::vector<T> &elems, 
                                              Random &generator) {
  return elems.begin() + std::uniform_int_distribution<>(0, (int)elems.size() - 1)(generator);
}

template<typename Random>
storage::BlockLayout RandomLayout(Random &generator) {
  uint16_t num_attrs = std::uniform_int_distribution<>(1, UINT16_MAX)(generator);
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
