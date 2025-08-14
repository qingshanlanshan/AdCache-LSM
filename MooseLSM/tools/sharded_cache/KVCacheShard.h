#include "RangeCacheShard.h"

class KVCacheShard : public RangeCacheShard {
 public:
  KVCacheShard(size_t capacity, std::shared_ptr<EvictionPolicy> policy)
      : RangeCacheShard(capacity, std::move(policy)) {}

  std::optional<V> get(K key) override { return RangeCacheShard::get(key); }

  bool put(K key, const V& value,
           std::optional<K> hint = std::nullopt) override {
    return RangeCacheShard::put(key, value, std::nullopt);
  }

  bool update(K key, const V& value) override {
    return RangeCacheShard::update(key, value);
  }

  size_t scan(K start_key, size_t length) override { return 0; }
  void set_capacity(size_t capacity) override {
    RangeCacheShard::set_capacity(capacity);
  }
};
