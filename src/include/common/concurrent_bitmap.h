#pragma once
#include <atomic>

#ifndef BYTE_SIZE
#define BYTE_SIZE 8u
#endif

static_assert(BYTE_SIZE == 8u, "BYTE_SIZE should be set to 8!");

#define ONE_HOT_MASK(n) (1u << (BYTE_SIZE - n - 1u))
#define ONE_COLD_MASK(n) (0xFF ^ ONE_HOT_MASK(n))

namespace noisepage {
// TODO: why constexpr?
constexpr uint32_t BitmapSize(uint32_t n) {
  return n % BYTE_SIZE == 0 ? n / BYTE_SIZE : n / BYTE_SIZE + 1;
}

class RawConcurrentBitmap {
public:
  bool Test(uint32_t pos) const {
    auto elem = bits_[pos / BYTE_SIZE].load();
    return static_cast<bool>(elem & ONE_HOT_MASK(pos % BYTE_SIZE));
  }

  bool operator[](uint32_t pos) const { return Test(pos); }

  bool Flip(uint32_t pos, bool expected) {
    auto mask = ONE_HOT_MASK(pos % BYTE_SIZE);
    // 这里用for循环是有道理的，多个thread可能同时修改同一byte的不同bit
    for (auto old_val = bits_[pos / BYTE_SIZE].load();
         static_cast<bool>(old_val & mask) == expected;
         old_val = bits_[pos / BYTE_SIZE].load()) {
      if (bits_[pos / BYTE_SIZE].compare_exchange_strong(old_val,
                                                         old_val ^ mask)) {
        return true;
      }
    }
    return false;
  }

private:
  std::atomic<uint8_t> bits_[0];
};

template <uint32_t N> class ConcurrentBitmap : public RawConcurrentBitmap {
private:
  std::atomic<uint8_t> bits_[BitmapSize(N)]{};
};

} // namespace noisepage
