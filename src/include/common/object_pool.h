#pragma once

#include "common/concurrent_queue.h"

namespace noisepage
{
template<typename T>
class ObjectPool {
  public:
    ObjectPool(uint32_t reuse_limit) : reuse_limit_(reuse_limit) {}

    ~ObjectPool() {
      T *elem;
      while (queue_.Dequeue(elem)) {
        delete elem;
      }
    }

    T *Get() {
      if (queue_.Empty()) {
        return new T();
      }
      T *elem;
      queue_.Dequeue(elem);
      return elem;
    }

    void Release(T *elem) {
      if (queue_.UnsafeSize() >= reuse_limit_) {
        delete elem;
      } else {
        queue_.Enqueue(std::move(elem));
      }
    }
  private:
    const uint32_t reuse_limit_;
    ConcurrentQueue<T *> queue_;
};
} // namespace noisepage
