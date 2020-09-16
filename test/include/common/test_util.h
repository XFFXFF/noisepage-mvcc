#pragma once

#include <random>
#include <functional>
#include <thread>
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

} // namespace testutil
} // namespace noisepage
