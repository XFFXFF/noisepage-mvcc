#pragma
#include <tbb/concurrent_unordered_map.h>

namespace noisepage
{
template<typename K,
         typename V,
         typename Hasher = tbb::tbb_hash<K>,
         typename Equality = std::equal_to<K>,
         typename Alloc = tbb::tbb_allocator<std::pair<const K, V>>>
class ConcurrentMap {
  public:
    bool Insert(const K &key, V &value) {
      return map_.insert(std::make_pair(key, value)).second;
    }

    bool Find(const K &key, V &value) {
      auto it = map_.find(key);
      if (it == map_.end()) {
        return false;
      }
      value = it->second;
      return true;
    }

    void UnsafeErase(const K &key) {
      map_.unsafe_erase(key);
    }
  private:
    tbb::concurrent_unordered_map<K, V, Hasher, Equality, Alloc> map_;
};
} // namespace noisepage
