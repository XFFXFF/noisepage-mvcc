#pragma once
#include <tbb/concurrent_queue.h>

namespace noisepage {
template <typename T> class ConcurrentQueue {
public:
  bool Empty() { return queue_.empty(); }

  void Enqueue(T &&elem) { queue_.push(elem); }

  bool Dequeue(T &elem) { return queue_.try_pop(elem); }

  uint32_t UnsafeSize() { return queue_.unsafe_size(); }

private:
  tbb::concurrent_queue<T> queue_;
};

} // namespace noisepage
