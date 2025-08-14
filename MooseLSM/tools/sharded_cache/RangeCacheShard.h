#pragma once

#include <cassert>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <variant>

#include "EvictionPolicy.h"
#include "ShardedCacheBase.h"
#include "SkipList.h"

class RangeCacheShard : public CacheShardBase {
 public:
  RangeCacheShard(size_t capacity, std::shared_ptr<EvictionPolicy> policy)
      : CacheShardBase(capacity, nullptr),
        eviction_policy_(std::move(policy)) {}

  std::optional<V> get(K key) override {
    std::lock_guard<std::mutex> lock(mutex_);
    V ret;
    auto it = map_.find(key);
    if (it == map_.end()) return std::nullopt;
    if (std::holds_alternative<SkipList::Node*>(it->second)) {
      auto node = std::get<SkipList::Node*>(it->second);
      ret = *node->value;
    } else {
      ret = *std::get<std::shared_ptr<V>>(it->second);
    }
    request(key);
    return ret;
  }
  // if hint is provided, it is part of a range put
  bool put(K key, const V& value, std::optional<K> hint) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      if (!hint) {  // insert to map only
        Entry entry = std::make_shared<V>(value);
        map_[key] = std::move(entry);
        request(key);
        return true;
      } else {
        // look for hint node
        auto hint_it = map_.find(*hint);
        if (hint_it == map_.end()) {
          // should not happen
          return false;
        }
        SkipList::Node* hint_node = nullptr;
        hint_node = promote_to_node(hint_it->first, hint_it->second);
        hint_node->next_key = true;  // indicate adjacent next key

        // insert new node after hint
        auto new_node =
            skip_list_.insert_after(hint_node, key, std::make_shared<V>(value));
        assert(new_node);
        map_[key] = new_node;

        request(key);
        return true;
      }
    } else {
      if (!hint) {
        // update existing value
        if (std::holds_alternative<SkipList::Node*>(it->second)) {
          auto node = std::get<SkipList::Node*>(it->second);
          node->value = std::make_shared<V>(value);
        } else {
          it->second = std::make_shared<V>(value);
        }
        request(key);
        return true;
      } else {
        auto hint_it = map_.find(*hint);
        if (hint_it == map_.end()) {
          // should not happen
          return false;
        }
        // update previous node
        SkipList::Node* hint_node = nullptr;
        hint_node = promote_to_node(hint_it->first, hint_it->second);
        hint_node->next_key = true;  // indicate adjacent next key

        // update existing node
        if (std::holds_alternative<SkipList::Node*>(it->second)) {
          auto node = std::get<SkipList::Node*>(it->second);
          node->value = std::make_shared<V>(value);
        } else {
          it->second = std::make_shared<V>(value);
          promote_to_node(key, it->second);
        }
        request(key);
        return true;
      }
    }
  }
  bool update(K key, const V& value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    if (std::holds_alternative<SkipList::Node*>(it->second)) {
      auto node = std::get<SkipList::Node*>(it->second);
      node->value = std::make_shared<V>(value);
    } else {
      it->second = std::make_shared<V>(value);
    }
    request(key);
    return true;
  }

  size_t scan(K start_key, size_t length) override {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t count = 0;
    auto node = skip_list_.find_node(start_key);
    if (!node) return 0;
    bool in_range = false;
    if (node->key > start_key) {
      auto prev_node = skip_list_.prev(node);
      if (prev_node && prev_node->next_key)
        in_range = true;
    } else if (node->next_key) {
      in_range = true;
    }
    if (!in_range) return 0;

    while (node && count < length) {
      count++;
      request(node->key);
      if (node->next_key)
        node = skip_list_.next(node);
      else
        break;
    }
    return count;
  }

  void set_capacity(size_t capacity) override {
    std::lock_guard<std::mutex> lock(mutex_);
    capacity_ = capacity;
    auto evicted_keys = eviction_policy_->set_capacity(capacity);
    for (const auto& key : evicted_keys) {
      remove(key);
    }
  }

  size_t get_capacity() const override {
    std::lock_guard<std::mutex> lock(mutex_);
    return capacity_;
  }

 private:
  using Entry = std::variant<SkipList::Node*, std::shared_ptr<V>>;

  SkipList::Node* promote_to_node(K key, Entry& entry) {
    // assert lock is held
    SkipList::Node* node = nullptr;
    if (std::holds_alternative<SkipList::Node*>(entry)) {
      node = std::get<SkipList::Node*>(entry);
    } else {
      // hint is a shared_ptr, we need to create a new node
      auto hint_value_ptr = std::get<std::shared_ptr<V>>(entry);
      node = skip_list_.insert(key, hint_value_ptr);
      entry = node;
    }
    return node;
  }

  void remove(K key) {
    // assert lock is held
    auto it = map_.find(key);
    if (it == map_.end()) return;
    SkipList::Node* node = nullptr;
    if (std::holds_alternative<SkipList::Node*>(it->second)) {
      node = std::get<SkipList::Node*>(it->second);
      skip_list_.remove(node);
    }
    map_.erase(it);
  }

  void request(K key) {
    // assert lock is held
    auto evicted = eviction_policy_->request(key);
    if (evicted) {
      remove(*evicted);
    }
  }

  std::unordered_map<K, Entry> map_;
  SkipList skip_list_;
  mutable std::mutex mutex_;
  std::shared_ptr<EvictionPolicy> eviction_policy_;
};