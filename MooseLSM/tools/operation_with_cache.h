#pragma once

#include <atomic>
#include <unordered_map>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "tools/sharded_cache/ShardedCacheBuilder.h"

using namespace std;

inline std::string ItoaWithPadding(const uint64_t key, uint64_t size) {
  std::string key_str = std::to_string(key);
  std::string padding_str(size - key_str.size(), '0');
  key_str = padding_str + key_str;
  return key_str;
}

struct AdCache_Helper {
  std::unordered_map<K, size_t> frequency_map;
  size_t total_frequency = 0;
  std::mutex mutex;
} adcache_helper;

rocksdb::Status get_with_cache(rocksdb::DB* db, std::optional<ShardedCacheBase> cache, std::string& key_string, double threshold) {
  size_t key = std::stoull(key_string);
  stats.OP_count++;
  stats.n_get++;
  auto start = chrono::steady_clock::now();
  if (cache && cache->get(key)) {
    auto duration = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
    stats.get_time += duration;
    stats.OP_time += duration;
    stats.n_hit++;
    return rocksdb::Status::OK();
  }
  std::string value;
  auto status = db->Get(rocksdb::ReadOptions(), key_string, &value);
  if (!status.ok()) 
    // should not happen
    return status;

  if (!cache) {
    // block cache, do nothing
  }
  else if (cache->name == "adcache") {
    bool allowed = false;
    {
      // frequency-based admission control
      std::lock_guard<std::mutex> lock(adcache_helper.mutex);
      adcache_helper.frequency_map[key] += 1;
      adcache_helper.total_frequency += 1;
      if (adcache_helper.frequency_map[key] >= threshold * adcache_helper.total_frequency) 
        allowed = true;
      if (adcache_helper.frequency_map[key] > 8) {
        allowed = true;
        for (auto& pair : adcache_helper.frequency_map) {
          pair.second /= 2;
        }
      }
    }
    if (allowed) {
      cache->put(key, value);
    }
  }
  else {
   cache->put(key, value);
  }

  auto duration = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
  stats.get_time += duration;
  stats.OP_time += duration;
  return status;
}

rocksdb::Status put_with_cache(rocksdb::DB* db, std::optional<ShardedCacheBase> cache, std::string& key_string,
                               std::string& value) {
  size_t key = std::stoull(key_string);
  stats.OP_count++;
  stats.n_put++;
  auto start = chrono::steady_clock::now();

  auto status = db->Put(rocksdb::WriteOptions(), key_string, value);
  if (!status.ok()) 
    // should not happen
    return status;

  if (cache) {
    cache->update(key, value);
  }
  auto duration = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
  stats.put_time += duration;
  stats.OP_time += duration;
  return status;
}

rocksdb::Status scan_with_cache(rocksdb::DB* db, std::optional<ShardedCacheBase> cache,
                                const std::string& key_string, size_t length, size_t cache_limit) {
  size_t key = std::stoull(key_string);
  stats.OP_count++;
  stats.n_scan++;
  size_t count = 0;

  if (cache) {
    auto start = chrono::steady_clock::now();
    count = cache->scan(key, length);
    auto duration = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
    stats.scan_time += duration;
    stats.OP_time += duration;
    stats.time_in_cache += duration;
    if (count > 0) {
      stats.length_in_cache += count;
      stats.n_in_cache++;
      stats.scan_length += count;
    }
    if (count >= length) {
      return rocksdb::Status::OK();
    }
    else {
      key += count;
    }
  }

  std::string scan_key = ItoaWithPadding(key, 16);
  auto start = chrono::steady_clock::now();
  auto iter = db->NewIterator(rocksdb::ReadOptions());
  iter->Seek(scan_key);
  for (size_t i = 0; i < length - count; ++i, ++key, iter->Next()) {
    if (!iter->Valid()) break;
    std::string value = iter->value().ToString();
    if (cache) {
      if (i == 0 && count == 0)
        cache->put(key, value);
      else
        cache->put(key, value, key - 1);
    }
  }
  delete iter;

  auto duration = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
  stats.scan_time += duration;
  stats.OP_time += duration;
  stats.scan_length += length - count;
  stats.length_in_db += length - count;
  stats.n_in_db++;
  stats.time_in_db += duration;
  
  return rocksdb::Status::OK();
}
