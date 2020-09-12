#pragma once

#include "common/concurrent_queue.h"

namespace noisepage
{
template<typename T>
class ObjectPool {
  public:
    explicit ObjectPool(uint32_t reuse_limit) : reuse_limit_(reuse_limit) {}

    ~ObjectPool() {
      T *obj;
      while (queue_.Dequeue(obj)) {
        delete obj;
      }
    }

    T *Get() {
      T *result;
      return queue_.Dequeue(result) ? result : new T(); 
    }

    void Release(T *obj) {
      if (queue_.UnsafeSize() > reuse_limit_) {
        delete obj;
      } else {
        queue_.Enqueue(std::move(obj));
      }
    }
  private:
    const uint32_t reuse_limit_;
    ConcurrentQueue<T *> queue_;
};
} // namespace noisepage
