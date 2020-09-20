#pragma once

#include <random>
#include <functional>
#include <thread>

namespace noisepage
{
namespace testutil
{
template<typename T, typename Random>
typename std::vector<T>::iterator UniformRandomElement(std::vector<T> &elems, 
                                              Random &generator) {
  return elems.begin() + std::uniform_int_distribution<>(0, (int)elems.size() - 1)(generator);
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
