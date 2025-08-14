#include "EvictionPolicy.h"
#include <deque>
#include <unordered_map>
#include <queue>
#include <optional>
#include <cmath>
#include <random>

class LeCaR : public EvictionPolicy {
public:
    LeCaR(size_t capacity)
        : EvictionPolicy(capacity), cache_size(capacity), history_size(capacity / 2),
          W{0.5f, 0.5f}, learning_rate(0.45f),
          discount_rate(std::pow(0.005f, 1.0f / capacity)), time(0) {
        rng.seed(123);
    }

    std::optional<K> request(K key) override {
        ++time;
        if (cache.count(key)) {
            hit(key);
            return std::nullopt;
        } else {
            return miss(key);
        }
    }

    std::vector<K> set_capacity(size_t capacity) override {
        std::vector<K> evicted_keys;
        cache_size = capacity;
        history_size = capacity / 2;
        return evicted_keys;  // nothing forcibly evicted
    }

    bool warmup_done() override {
        return cache.size() >= cache_size;
    }

private:
    struct Entry {
        K key;
        int freq;
        size_t time;
        size_t evicted_time;

        bool operator<(const Entry& other) const {
            return freq > other.freq || (freq == other.freq && time > other.time);
        }
    };

    size_t cache_size;
    size_t history_size;
    float W[2];
    float learning_rate;
    float discount_rate;
    size_t time;
    std::default_random_engine rng;
    std::uniform_real_distribution<float> dist{0.0f, 1.0f};

    std::unordered_map<K, Entry> cache;
    std::unordered_map<K, Entry> lru_hist, lfu_hist;
    std::deque<K> lru_order;
    std::priority_queue<Entry> lfu_heap;

    bool in_lfu(const K& key) {
        return cache.find(key) != cache.end();
    }

    void hit(K key) {
        auto& entry = cache[key];
        entry.freq++;
        entry.time = time;
        lru_order.erase(std::find(lru_order.begin(), lru_order.end(), key));
        lru_order.push_back(key);
        lfu_heap.push(entry);
    }

    std::optional<K> miss(K key) {
        int freq = 1;
        if (lru_hist.count(key)) {
            freq = lru_hist[key].freq + 1;
            float reward = -std::pow(discount_rate, time - lru_hist[key].evicted_time);
            adjust_weights(reward, 0);
            lru_hist.erase(key);
        } else if (lfu_hist.count(key)) {
            freq = lfu_hist[key].freq + 1;
            float reward = -std::pow(discount_rate, time - lfu_hist[key].evicted_time);
            adjust_weights(0, reward);
            lfu_hist.erase(key);
        }

        std::optional<K> evicted;
        if (cache.size() >= cache_size) {
            evicted = evict();
        }

        Entry new_entry{key, freq, time, 0};
        cache[key] = new_entry;
        lru_order.push_back(key);
        lfu_heap.push(new_entry);

        return evicted;
    }

    K get_lru() {
        return lru_order.front();
    }

    Entry get_lfu() {
        while (!lfu_heap.empty() && !cache.count(lfu_heap.top().key)) {
            lfu_heap.pop();
        }
        return lfu_heap.top();
    }

    int get_choice() {
        return dist(rng) < W[0] ? 0 : 1;
    }

    K evict() {
        K lru_key = get_lru();
        Entry lfu_entry = get_lfu();

        int policy = get_choice();
        K victim;

        if (lru_key == lfu_entry.key) {
            victim = lru_key;
            policy = -1;
        } else {
            victim = (policy == 0) ? lru_key : lfu_entry.key;
        }

        Entry evicted_entry = cache[victim];
        evicted_entry.evicted_time = time;
        cache.erase(victim);
        lru_order.erase(std::find(lru_order.begin(), lru_order.end(), victim));

        if (policy == 0) {
            insert_history(lru_hist, victim, evicted_entry);
        } else if (policy == 1) {
            insert_history(lfu_hist, victim, evicted_entry);
        }

        return victim;
    }

    void insert_history(std::unordered_map<K, Entry>& hist, K key, Entry entry) {
        if (hist.size() >= history_size) {
            auto oldest = hist.begin();
            for (auto it = hist.begin(); it != hist.end(); ++it) {
                if (it->second.time < oldest->second.time) {
                    oldest = it;
                }
            }
            hist.erase(oldest);
        }
        hist[key] = entry;
    }

    void adjust_weights(float r_lru, float r_lfu) {
        float e_lru = std::exp(learning_rate * r_lru);
        float e_lfu = std::exp(learning_rate * r_lfu);
        float sum = W[0] * e_lru + W[1] * e_lfu;
        W[0] = (W[0] * e_lru) / sum;
        W[1] = (W[1] * e_lfu) / sum;
        if (W[0] > 0.99f) W[0] = 0.99f, W[1] = 0.01f;
        if (W[1] > 0.99f) W[1] = 0.99f, W[0] = 0.01f;
    }
};
