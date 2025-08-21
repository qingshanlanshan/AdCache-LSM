#pragma once

#include <memory>      
#include <optional>    // for std::optional
#include "Utils.h"     // must define using K = ...; using V = ...;
#include "EvictionPolicy.h"  // or forward-declare: class EvictionPolicy;
#include "SkipList.h"

class CacheShardBase {
public:
  virtual ~CacheShardBase() = default;

  // API
  virtual std::optional<V> get(K key) = 0;
  virtual bool put(K key, const V& value, std::optional<K> hint = std::nullopt) = 0;
  virtual bool update(K key, const V& value) = 0;
  virtual size_t scan(K start_key, size_t length) = 0;
  // AdCache specific
  virtual void set_capacity(size_t capacity) = 0;
  virtual size_t get_capacity() const = 0;
  virtual bool warmup_done() const {return true;}
  virtual std::optional<K> peek_victim() { return std::nullopt; }

  CacheShardBase(const CacheShardBase&) = delete;
  CacheShardBase& operator=(const CacheShardBase&) = delete;
  CacheShardBase(CacheShardBase&&) = default;
  CacheShardBase& operator=(CacheShardBase&&) = default;

protected:
  explicit CacheShardBase(size_t capacity, std::shared_ptr<EvictionPolicy> policy)
      : capacity_(capacity), policy_(policy) {}

  size_t capacity_;
  std::shared_ptr<EvictionPolicy> policy_;

  EvictionPolicy& policy()             { return *policy_; }
  const EvictionPolicy& policy() const { return *policy_; }
};
