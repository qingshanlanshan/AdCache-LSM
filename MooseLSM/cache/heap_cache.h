//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#define HEAP_CACHE_LOGGING 0
#define BLOCK_CACHE 0
#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "cache/secondary_cache_adapter.h"
#include "cache/sharded_cache.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/malloc.h"
#include "port/port.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"

namespace ROCKSDB_NAMESPACE {
namespace heap_cache {

using std::string;

size_t simple_hash(std::string key) {
  size_t hash = 0;
  for (size_t i = 0; i < key.size(); i++) {
    hash = hash * 31 + key[i];
  }
  return hash;
}

enum CacheItemType {
  kCacheKV,
  kCacheKP,
  kCacheBlock,
};

struct CacheItem {
  float cost;
  size_t frequency;
  size_t size;
  float priority;
  CacheItemType type;
  std::string key;
  int level;
  const Cache::CacheItemHelper* helper;
  uint32_t GetHash() const { return 0; }

  CacheItem() {}
  CacheItem(float p) : priority(p) {}
  ~CacheItem() {
    if (helper && helper->need_free) {
      delete helper;
    }
  }
};

using HeapCacheHandle = CacheItem;

struct CacheItemKV : public CacheItem {
  string value;
};

struct CacheItemBlock : public CacheItem {
  Cache::ObjectPtr block;
};

struct LinkedNode {
  LinkedNode* prev;
  LinkedNode* next;
  CacheItem* item;
  ~LinkedNode() { delete item; }
};
class LinkedList {
 public:
  LinkedNode* head;
  LinkedNode* tail;
  size_t len;
  LinkedList() {
    head = new LinkedNode();
    tail = new LinkedNode();
    head->next = tail;
    tail->prev = head;
    len = 0;
  }
  ~LinkedList() {
    while (head->next != tail) {
      LinkedNode* node = head->next;
      head->next = node->next;
      delete node;
    }
    delete head;
    delete tail;
  }
  void append(LinkedNode* node) {
    node->prev = tail->prev;
    node->next = tail;
    tail->prev->next = node;
    tail->prev = node;
    len++;
  }
  void append_front(LinkedNode* node) {
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;
    len++;
  }
  void remove(LinkedNode* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    len--;
  }
  LinkedNode* pop() {
    LinkedNode* node = head->next;
    if (node == tail) return nullptr;
    remove(node);
    return node;
  }
  LinkedNode* front() { return head->next; }
};

class CacheImpl {
 public:
  CacheImpl() {}
  virtual ~CacheImpl() {}

  virtual CacheItem* get(string key, bool is_kv = false) = 0;
  virtual void put(CacheItem* item) = 0;
  virtual size_t evict(uint64_t size) = 0;
  virtual size_t get_capacity() const = 0;
  virtual size_t get_size() const = 0;
  virtual void modify_size(int diff) = 0;
  virtual void report() {}
  virtual void set_next_victim(string key) = 0;
};
class HeapCacheImpl : public CacheImpl {
 public:
  HeapCacheImpl(uint64_t cap);
  ~HeapCacheImpl();
  bool heap_check();
  CacheItem* top()
  { 
    if (pq.size() == 0) return nullptr;
    return pq[0]; 
  }
  CacheItem* get(string key, bool is_kv) override;
  size_t evict(uint64_t size) override;
  void put(CacheItem* item) override;
  void report() override;
  size_t get_capacity() const override { return capacity; }
  size_t get_size() const override { return current_size; }
  void modify_size(int diff) override { current_size += diff; }
  void set_next_victim(string key) override;

 private:
  std::vector<CacheItem*> pq;
  std::unordered_map<std::string, int> table;

  float logical_clock;
  size_t capacity;
  size_t current_size;
  void heap_swap(size_t i, size_t j);
  void move_down(size_t idx);
  void move_up(size_t idx);
  void heapify(size_t idx);
  void push_heap() { move_up(pq.size() - 1); }
  CacheItem* pop_heap();
  float calculatePriority(CacheItem& item);
  int evict_count=0;
};

class ELRUCacheImpl : public CacheImpl {
 public:
  ELRUCacheImpl(uint64_t cap, size_t n = 1)
      : capacity(cap), current_size(0), window_size(n) {
    if (window_size <= 0) window_size = 1;
  }
  ~ELRUCacheImpl() {}
  CacheItem* get(string key, bool is_kv) override;
  size_t evict(uint64_t size) override;
  void put(CacheItem* item) override;
  size_t get_capacity() const override;
  size_t get_size() const override;
  void modify_size(int diff) override;
  void report() override;
  void set_next_victim(string key) override;
  void print()
  {
    auto node = list.front();
    while (node != list.tail)
    {
      std::cout<<simple_hash(node->item->key)<<" "<<node->item->priority<<std::endl;
      node = node->next;
    }
    std::cout<<std::endl;
  }

 private:
  LinkedList list;
  std::unordered_map<std::string, LinkedNode*> hash;
  size_t capacity;
  size_t current_size;
  size_t window_size;
  int evict_count = 0;
};

class LevelCacheImpl : public CacheImpl {
 public:
  LevelCacheImpl(uint64_t cap);
  ~LevelCacheImpl();
  CacheItem* get(string key, bool is_kv) override;
  size_t evict(uint64_t size) override;
  void put(CacheItem* item) override;
  size_t get_capacity() const override;
  size_t get_size() const override;
  void modify_size(int diff) override;
  void report() override;
  void set_next_victim(string key) override;
  float calculatePriority(CacheItem& item);
  size_t level_evict(uint64_t size);

 private:
  size_t capacity;
  size_t current_size;
  std::vector<HeapCacheImpl*> levels;
};

class CountMinSketch {
 public:
  CountMinSketch(int n_hash, int n_bucket);
  void dump_sketch();
  void reset_sketch();
  void insert(std::string key, int count);
  size_t query(std::string key);
  uint32_t hash(int i, std::string key);
  int get_sum_counter();

 private:
  std::vector<std::vector<uint8_t>> sketch;
  std::vector<uint32_t> hash_seed;
  int n_hash_;
  int n_bucket_;
  int sum_counter;
  bool reseted;
};

class HeapCacheShard final : public CacheShardBase {
 public:
  using HandleImpl = HeapCacheHandle;
  using HashVal = uint32_t;
  using HashCref = uint32_t;
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return Lower32of64(GetSliceNPHash64(key, seed));
  }
  HeapCacheShard(const ShardedCacheOptions& opts, size_t capacity)
      : CacheShardBase(opts.metadata_charge_policy), cmsketch(4, 256) {
    if (BLOCK_CACHE)
      cache_ = new ELRUCacheImpl(capacity, 1);
    else
      // cache_ = new HeapCacheImpl(capacity);
    cache_ = new ELRUCacheImpl(capacity, 1);
    // cache_ = new LevelCacheImpl(capacity);
  }
  ~HeapCacheShard() { delete cache_; }
  struct {
    int n_block_put = 0;
    int n_block_hit = 0;
    int n_block_get = 0;
    int n_kv_hit = 0;
    int n_kv_put = 0;
    int n_kv_get = 0;
    double get_duration = 0;
    double put_duration = 0;
  } stats_;
  void report() {
    // stats
    std::cout << "cache put: " << stats_.n_kv_put + stats_.n_block_put
              << "\tcache get: " << stats_.n_block_get + stats_.n_kv_get
              << "\tcache hit: " << stats_.n_kv_hit + stats_.n_block_hit
              << "\tcache hit rate: " << get_hit_rate() 
              << std::endl;
    std::cout << "   kv put: " << stats_.n_kv_put
              << "\t   kv get: " << stats_.n_kv_get
              << "\t   kv hit: " << stats_.n_kv_hit 
              << "\t   kv hit rate: " << (float)stats_.n_kv_hit / stats_.n_kv_get 
              << std::endl;
    std::cout << "block put: " << stats_.n_block_put
              << "\tblock get: " << stats_.n_block_get
              << "\tblock hit: " << stats_.n_block_hit 
              << "\tblock hit rate: " << (float)stats_.n_block_hit / stats_.n_block_get 
              << std::endl;

    std::cout << "avg get duration: " << stats_.get_duration / (stats_.n_kv_get + stats_.n_block_get)
              << "\tavg put duration: " << stats_.put_duration / (stats_.n_kv_put + stats_.n_block_put)
              << std::endl;

    cache_->report();
  }

  void reset_stats() {
    stats_.n_kv_put = 0;
    stats_.n_kv_hit = 0;
    stats_.n_block_put = 0;
    stats_.n_block_hit = 0;
    stats_.n_kv_get = 0;
    stats_.n_block_get = 0;
    stats_.get_duration = 0;
    stats_.put_duration = 0;
  }

  double get_hit_rate() {
    if (stats_.n_kv_hit > 0) {
      return (float)(stats_.n_kv_hit + stats_.n_block_hit) /
             (stats_.n_kv_get + stats_.n_block_get);
    }
    else {
      return (float)stats_.n_block_hit / stats_.n_block_get;
    }
  }

  Status Insert(const Slice& key, HashVal hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                HeapCacheHandle** handle, Cache::Priority priority) {
    (void)priority;
    CacheItem* new_item = nullptr;
    bool no_put = false;
    DMutexLock l(mutex_);
    auto start = std::chrono::steady_clock::now();
    string k = key.ToString();
    if (helper->type == Cache::CacheItemHelper::CacheItemType::kCacheKV) {
      bool put_kv = !BLOCK_CACHE;
      if (helper->cost > 0) {
        size_t freq = 1;
        // {
        //   cmsketch.insert(k, 1);
        //   freq = cmsketch.query(k);
        //   float ratio = (float)freq / cmsketch.get_sum_counter();
        //   if (ratio > 0.005) {
        //     put_kv = true;
        //   }
        // }
        if (put_kv) {
          stats_.n_kv_put++;
          CacheItemKV* item = new CacheItemKV();
          item->size = charge;
          item->cost = helper->cost;
          item->frequency = 1;
          item->key = k;
          item->value = *static_cast<std::string*>(value);
          item->helper = helper;
          item->type = kCacheKV;
          item->level = helper->level;
          new_item = item;
        }
      }
      if (!put_kv && helper && helper->need_free) delete helper;
    } else {
      stats_.n_block_put++;
      CacheItemBlock* item = new CacheItemBlock();
      item->size = charge;
      item->cost = 1;
      item->frequency = 1;
      item->key = k;
      item->block = value;
      item->helper = helper;
      item->type = kCacheBlock;
      item->level = helper->level;
      new_item = item;
      if (item->block == nullptr) no_put = true;
    }

    if (first && new_item &&
        new_item->size + cache_->get_size() > cache_->get_capacity()) {
      if (HEAP_CACHE_LOGGING)
        std::cout << "warmup done" << std::endl;
      first = false;
      reset_stats();
    }

    if (no_put && new_item) {
      // cache_->evict(charge);
      // cache_->modify_size(charge);
      (void)no_put;
    } else if (new_item) {
      cache_->put(new_item);
    }
    auto end = std::chrono::steady_clock::now();
    stats_.put_duration +=
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    if (handle) *handle = new_item;
    return Status::OK();
  }
  HeapCacheHandle* CreateStandalone(const Slice& key, HashVal hash,
                                    Cache::ObjectPtr obj,
                                    const Cache::CacheItemHelper* helper,
                                    size_t charge, bool allow_uncharged) {
    return nullptr;
  }
  HeapCacheHandle* Lookup(const Slice& key, HashVal hash,
                          const Cache::CacheItemHelper* helper,
                          Cache::CreateContext* create_context,
                          Cache::Priority priority, Statistics* stats) {
    (void)helper, (void)create_context, (void)priority, (void)stats;
    string k = key.ToString();
    HeapCacheHandle* handle = nullptr;
    if (k == "report") {
      report();
    } else if (k == "compact") {
      if (HEAP_CACHE_LOGGING) std::cout << "compact" << std::endl;
    } else if (k == "compact_done") {
      if (HEAP_CACHE_LOGGING) std::cout << "compact_done" << std::endl;
    } else {
      DMutexLock l(mutex_);
      auto start = std::chrono::steady_clock::now();
      bool lookup_kv =
          helper &&
          helper->type == Cache::CacheItemHelper::CacheItemType::kCacheKV;
      stats_.n_kv_get += lookup_kv;
      stats_.n_block_get += !lookup_kv;

      handle = cache_->get(k, lookup_kv);

      stats_.n_kv_hit += (handle && lookup_kv);
      stats_.n_block_hit += (handle && !lookup_kv);
      auto end = std::chrono::steady_clock::now();
      stats_.get_duration +=
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
      // for each several get, output and reset the hit rate
      if (HEAP_CACHE_LOGGING && (stats_.n_kv_get + stats_.n_block_get) >= 10000) {
        std::cout << "hit rate: " << get_hit_rate() << std::endl;
        std::cout << "kv hit rate: " << (float)stats_.n_kv_hit / stats_.n_kv_get
                  << std::endl;
        std::cout << "block hit rate: "
                  << (float)stats_.n_block_hit / stats_.n_block_get
                  << std::endl;
        reset_stats();
      }
    }
    return handle;
  }
  bool Release(HeapCacheHandle* handle, bool useful, bool erase_if_last_ref) {
    if (handle == nullptr) return false;
    {
      DMutexLock l(mutex_);
      auto h = cache_->get(handle->key);
      if (!h)
      {
        // assert(cache_->get_size() > handle->size);
        // cache_->modify_size(-handle->size);
      }
      else if (h && h->type == kCacheBlock && erase_if_last_ref) {
        cache_->set_next_victim(handle->key);
      }
    }
    return true;
  }
  bool Ref(HeapCacheHandle* handle) { return true; }
  void Erase(const Slice& key, HashVal hash) {}
  void SetCapacity(size_t capacity) {}
  void SetStrictCapacityLimit(bool strict_capacity_limit) {}
  size_t GetUsage() const { return cache_->get_size(); }
  size_t GetPinnedUsage() const { return 0; }
  size_t GetOccupancyCount() const { return 0; }
  size_t GetTableAddressCount() const { return 0; }
  // Handles iterating over roughly `average_entries_per_lock` entries, using
  // `state` to somehow record where it last ended up. Caller initially uses
  // *state == 0 and implementation sets *state = SIZE_MAX to indicate
  // completion.
  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state) {
    *state = SIZE_MAX;
  }
  void EraseUnRefEntries() {}
  void AppendPrintableOptions(std::string& str) const {
    const int kBufferSize = 200;
    char buffer[kBufferSize];
    {
      DMutexLock l(mutex_);
      snprintf(buffer, kBufferSize, "capacity: %lu\n", cache_->get_capacity());
    }
    str.append(buffer);
  }
  static inline uint32_t HashPieceForSharding(HashCref hash) {
    return Lower32of64(hash);
  }

 private:
  CacheImpl* cache_;
  bool first = true;
  mutable DMutex mutex_;
  CountMinSketch cmsketch;
};

class HeapCache final : public ShardedCache<HeapCacheShard> {
 public:
  explicit HeapCache(const ShardedCacheOptions& opts) : ShardedCache(opts) {
    size_t per_shard = GetPerShardCapacity();
    InitShards(
        [&](HeapCacheShard* cs) { new (cs) HeapCacheShard(opts, per_shard); });
  }

  const char* Name() const override { return "HeapCache"; }

  size_t GetCharge(Cache::Handle* handle) const override {
    return reinterpret_cast<const HeapCacheHandle*>(handle)->size;
  }

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override {
    auto h = reinterpret_cast<const HeapCacheHandle*>(handle);
    return h->helper;
  }

  Cache::ObjectPtr Value(Handle* handle) {
    if (!handle) return nullptr;
    auto h = reinterpret_cast<HeapCacheHandle*>(handle);
    if (h->type == kCacheKV) {
      auto kv = static_cast<CacheItemKV*>(h);
      return &kv->value;
    } else if (h->type == kCacheBlock) {
      auto block = static_cast<CacheItemBlock*>(h);
      return block->block;
    }
    return nullptr;
  }
};

}  // namespace heap_cache

std::shared_ptr<Cache> HeapCacheOptions::MakeSharedCache() const {
  HeapCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  std::shared_ptr<Cache> cache = std::make_shared<heap_cache::HeapCache>(opts);
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE
