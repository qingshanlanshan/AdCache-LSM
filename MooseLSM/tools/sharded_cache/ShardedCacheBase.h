#pragma once

#include "Utils.h"
#include "CacheShardBase.h"
#include "SkipList.h"

class ShardedCacheBase {
 public:
  std::string name = "ShardedCacheBase";
  ShardedCacheBase(
      K range_start, K range_end, size_t capacity, size_t num_shards,
      std::function<std::shared_ptr<CacheShardBase>(size_t)> shard_factory) {
    if (num_shards == 0 || range_start >= range_end) {
      throw std::invalid_argument("Invalid shard configuration");
    }

    K total_range = range_end - range_start;
    K range_size = total_range / num_shards;

    K end = range_start;
    for (size_t i = 0; i < num_shards; ++i) {
      end += range_size;
      if (i == num_shards - 1) {
        end = range_end;  // Last shard takes the remaining range
        shards_[end] = shard_factory(capacity / num_shards + capacity % num_shards);
      }
      else
        shards_[end] = shard_factory(capacity / num_shards);
    }
  }

  std::optional<V> get(K key) {
    CacheShardBase* shard = find_shard(key);
    return shard ? shard->get(key) : std::nullopt;
  }

  bool put(K key, V& value, std::optional<K> hint = std::nullopt) {
    CacheShardBase* shard = find_shard(key);
    return shard ? shard->put(key, value, hint) : false;
  }

  bool update(K key, V& value) {
    CacheShardBase* shard = find_shard(key);
    return shard ? shard->update(key, value) : false;
  }

  size_t scan(K start_key, size_t length) {
    CacheShardBase* shard = find_shard(start_key);
    return shard ? shard->scan(start_key, length) : 0;
  }

  // the functions below are for adcache, others might not be implemented

  void set_capacity(size_t capacity) {
    for (auto& [_, shard] : shards_) {
      shard->set_capacity(capacity / shards_.size());
    }
  }

  size_t get_capacity() const {
    if (shards_.empty()) return 0;
    return shards_.begin()->second->get_capacity() * shards_.size();
  }

  bool warmup_done() const {
    for (const auto& [_, shard] : shards_) {
      if (!shard->warmup_done()) return false;
    }
    return true;
  }

 private:
  std::map<K, std::shared_ptr<CacheShardBase>> shards_;

  CacheShardBase* find_shard(K key) {
    auto it = shards_.lower_bound(key);
    if (it != shards_.end()) return it->second.get();
    if (it != shards_.begin()) --it;
    return it->second.get();
  }
};
