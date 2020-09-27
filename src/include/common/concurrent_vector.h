#pragma once

#include <tbb/concurrent_vector.h>

namespace noisepage {

template <typename T, class Alloc = tbb::cache_aligned_allocator<T>>
class ConcurrentVector {
public:
  class Iterator {
  public:
    explicit Iterator(typename tbb::concurrent_vector<T, Alloc>::iterator it)
        : it_(it){};

    T &operator*() const { return it_.operator*(); }

    Iterator &operator++() {
      ++it_;
      return *this;
    }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }

  private:
    typename tbb::concurrent_vector<T, Alloc>::iterator it_;
  };

  ConcurrentVector() = default;
  explicit ConcurrentVector(uint64_t size, const T &t = T())
      : vector_(size, t) {}

  void PushBack(const T &item) { vector_.push_back(item); }

  T &At(uint64_t index) { return vector_.at(index); }

  T &operator[](int64_t index) { return At(index); }

  Iterator Begin() { return Iterator(vector_.begin()); }

  Iterator End() { return Iterator(vector_.end()); }

private:
  tbb::concurrent_vector<T, Alloc> vector_;
};
} // namespace noisepage
