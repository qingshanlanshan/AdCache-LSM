#pragma once
#include <list>
#include <optional>
#include <unordered_map>
#include <vector>

#include "EvictionPolicy.h"  // base class
#include "Utils.h"           // defines K, V

class LRU : public EvictionPolicy {
 public:
  explicit LRU(size_t capacity)
      : EvictionPolicy(capacity), capacity_(capacity) {}

  // Touch/insert a key. If inserting and over capacity, evict LRU and return
  // it.
  std::optional<K> request(K key) override {
    auto it = pos_.find(key);
    if (it != pos_.end()) {
      // Key already present: move to front (most-recent)
      order_.splice(order_.begin(), order_, it->second);
      return std::nullopt;
    }

    // Insert as most-recent
    order_.push_front(key);
    pos_[key] = order_.begin();

    // Evict if over capacity
    if (pos_.size() > capacity_) {
      K victim = order_.back();
      order_.pop_back();
      pos_.erase(victim);
      return victim;  // may equal 'key' when capacity_ == 0
    }
    return std::nullopt;
  }

  // Change capacity; evict oldest until within capacity. Return all evicted
  // keys.
  std::vector<K> set_capacity(size_t capacity) override {
    capacity_ = capacity;
    std::vector<K> evicted;
    while (pos_.size() > capacity_) {
      K victim = order_.back();
      order_.pop_back();
      pos_.erase(victim);
      evicted.push_back(victim);
    }
    return evicted;
  }

  bool warmup_done() const override {
    return pos_.size() >= capacity_;
  }

  void erase(K key) {
    auto it = pos_.find(key);
    if (it == pos_.end()) return;
    order_.erase(it->second);
    pos_.erase(it);
  }

  bool contains(K key) const { return pos_.find(key) != pos_.end(); }
  size_t size() const { return pos_.size(); }
  size_t capacity() const { return capacity_; }

  std::optional<K> peek_victim() const override {
    if (pos_.size() < capacity_) return std::nullopt;
    return order_.back();
  }

 private:
  size_t capacity_;
  std::list<K> order_;  // MRU at front, LRU at back
  std::unordered_map<K, std::list<K>::iterator> pos_;  // key -> list iterator
};