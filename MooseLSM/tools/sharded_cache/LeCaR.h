#pragma once

#include "DequeDict.h"
#include "HeapDict.h"
#include <unordered_map>
#include <cassert>
#include <iostream>
#include <cmath>
#include <random>
#include <array>

class LeCaR_Policy {
private:
    struct Entry {
        size_t oblock_;
        int freq_;
        size_t time_;
        std::optional<size_t> evicted_time;

        Entry(size_t oblock, int freq, size_t time)
            : oblock_(oblock), freq_(freq), time_(time), evicted_time(std::nullopt) {}

        bool operator<(const Entry& other) const {
            if (freq_ == other.freq_) return oblock_ < other.oblock_;
            return freq_ < other.freq_;
        }
    };

    size_t time = 0;
    size_t cache_size_;

    DequeDict<size_t, Entry> lru;
    HeapDict<size_t, Entry> lfu;

    DequeDict<size_t, Entry> lru_hist;
    DequeDict<size_t, Entry> lfu_hist;

    float initial_weight = 0.5f;
    float learning_rate = 0.45f;
    float discount_rate;
    std::array<float, 2> W;

    std::mt19937 rng{123};
    std::uniform_real_distribution<float> dist{0.0f, 1.0f};

public:
    explicit LeCaR_Policy(size_t cache_size)
        : cache_size_(cache_size),
          discount_rate(std::pow(0.005f, 1.0f / cache_size)),
          W{initial_weight, 1.0f - initial_weight} {}

    bool contains(size_t oblock) const {
        return lru.contains(oblock);
    }

    bool cacheFull() const {
        return lru.size() == cache_size_;
    }

    void addToCache(size_t oblock, int freq) {
        Entry x(oblock, freq, time);
        lru.set(oblock, x);
        lfu.set(oblock, x);
    }

    void addToHistory(const Entry& x, int policy) {
        DequeDict<size_t, Entry>* policy_history = nullptr;
        if (policy == 0) policy_history = &lru_hist;
        else if (policy == 1) policy_history = &lfu_hist;
        else return;

        if (policy_history->size() == cache_size_) {
            const Entry& evicted = policy_history->first();
            policy_history->erase(evicted.oblock_);
        }
        policy_history->set(x.oblock_, x);
    }

    Entry getLRU(DequeDict<size_t, Entry>& dict) const {
        return dict.first();
    }

    Entry getHeapMin() const {
        return lfu.min();
    }

    int getChoice() {
        return dist(rng) < W[0] ? 0 : 1;
    }

    std::pair<std::optional<size_t>, int> evict() {
        Entry lru_entry = getLRU(lru);
        Entry lfu_entry = getHeapMin(); 

        Entry evicted = lru_entry;
        int policy = getChoice();

        if (&lru_entry == &lfu_entry) {
            policy = -1;
        } else if (policy == 1) {
            evicted = lfu_entry;
        }

        lru.erase(evicted.oblock_);
        lfu.erase(evicted.oblock_);

        Entry x = evicted;
        x.evicted_time = time;
        addToHistory(x, policy);

        return {x.oblock_, policy};
    }

    void hit(size_t oblock) {
        Entry x = lru.get(oblock);
        x.time_ = time;
        x.freq_ += 1;
        lru.set(oblock, x);
        lfu.set(oblock, x);
    }

    void adjustWeights(float rewardLRU, float rewardLFU) {
        float reward[2] = {rewardLRU, rewardLFU};
        W[0] *= std::exp(learning_rate * reward[0]);
        W[1] *= std::exp(learning_rate * reward[1]);
        float sum = W[0] + W[1];
        W[0] /= sum;
        W[1] /= sum;

        if (W[0] >= 0.99f) W = {0.99f, 0.01f};
        else if (W[1] >= 0.99f) W = {0.01f, 0.99f};
    }

    std::optional<size_t> miss(size_t oblock) {
        std::optional<size_t> evicted;
        int freq = 1;

        if (lru_hist.contains(oblock)) {
            Entry entry = lru_hist.get(oblock);
            freq = entry.freq_ + 1;
            lru_hist.erase(oblock);
            float reward = -std::pow(discount_rate, time - *entry.evicted_time);
            adjustWeights(reward, 0);
        } else if (lfu_hist.contains(oblock)) {
            Entry entry = lfu_hist.get(oblock);
            freq = entry.freq_ + 1;
            lfu_hist.erase(oblock);
            float reward = -std::pow(discount_rate, time - *entry.evicted_time);
            adjustWeights(0, reward);
        }

        if (lru.size() == cache_size_) {
            auto [evict_block, _] = evict();
            evicted = evict_block;
        }

        addToCache(oblock, freq);
        return evicted;
    }

    std::pair<bool, std::optional<size_t>> request(size_t oblock) {
        time++;
        if (contains(oblock)) {
            hit(oblock);
            return {false, std::nullopt};
        } else {
            return {true, miss(oblock)};
        }
    }
};


class LeCaR : public EvictionPolicy {
 public:
  explicit LeCaR(size_t capacity)
      : EvictionPolicy(capacity), policy_(LeCaR_Policy(capacity)) {}

  // Touch/insert a key. If inserting and over capacity, evict LRU and return
  // it.
  std::optional<K> request(K key) override {
    auto [_, evicted] = policy_.request(key);
    return evicted;
  }

 private:
    LeCaR_Policy policy_;  // LeCaR policy instance
};