#pragma once

#include "Utils.h"
#include "CacheShardBase.h"
#include "RangeCacheShard.h"
#include "LRU.h"
#include "LeCaR.h"
#include "Cacheus.h"
#include "KVCacheShard.h"

using ShardFactory = std::function<std::shared_ptr<CacheShardBase>(size_t)>;

class ShardedCacheBuilder {
  public:
    static std::optional<ShardedCacheBase> BuildShardedCache(K range_start, K range_end, size_t capacity, size_t num_shards, std::string type) {
      if (num_shards == 0 || capacity == 0 || range_start >= range_end) {
        return std::nullopt;
      }
      // convert to lower case
      std::transform(type.begin(), type.end(), type.begin(), ::tolower);
      ShardFactory shard_factory;
      if (type == "range" || type == "adcache") {
        // range cache
        shard_factory = [](size_t cap) -> std::shared_ptr<CacheShardBase> {
          return std::make_unique<RangeCacheShard>(cap, std::make_unique<LRU>(cap));
        };
      }
      else if (type == "lecar") {
        shard_factory = [](size_t cap) -> std::shared_ptr<CacheShardBase> {
          return std::make_unique<RangeCacheShard>(cap, std::make_unique<LeCaR>(cap));
        };
      }
      else if (type == "cacheus") {
        shard_factory = [](size_t cap) -> std::shared_ptr<CacheShardBase> {
          return std::make_unique<RangeCacheShard>(cap, std::make_unique<Cacheus>(cap));
        };
      }
      else if (type == "kv") {
        shard_factory = [](size_t cap) -> std::shared_ptr<CacheShardBase> {
          return std::make_unique<KVCacheShard>(cap, std::make_unique<LRU>(cap));
        };
      }
      else {
        return std::nullopt;
      }

      ShardedCacheBase sharded_cache(range_start, range_end, capacity, num_shards,
                                shard_factory);
      sharded_cache.name = type;
      return sharded_cache;
    }
};