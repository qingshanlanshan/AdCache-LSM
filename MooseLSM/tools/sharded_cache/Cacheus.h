// Cacheus_Policy.h
#pragma once

#include "DequeDict.h"
#include "HeapDict.h"
#include <unordered_map>
#include <cassert>
#include <cmath>
#include <random>
#include <optional>
#include <array>

class Cacheus_Policy {
private:
    struct Entry {
        size_t oblock_;
        int freq_;
        size_t time_;
        std::optional<size_t> evicted_time;
        bool is_demoted_;
        bool is_new_;

        Entry(size_t oblock, int freq, size_t time, bool is_new = true)
            : oblock_(oblock), freq_(freq), time_(time), is_demoted_(false), is_new_(is_new) {}

        bool operator<(const Entry& other) const {
            if (freq_ == other.freq_) return time_ > other.time_;
            return freq_ < other.freq_;
        }
    };

    class AdaptiveLearningRate {
    public:
        float learning_rate;
        float learning_rate_reset;
        float learning_rate_curr;
        float learning_rate_prev;

        size_t period_len_;
        int hitrate = 0;
        float hitrate_prev = 0.0;
        float hitrate_diff_prev = 0.0;
        int hitrate_zero_count = 0;
        int hitrate_nega_count = 0;

        AdaptiveLearningRate(size_t period_len)
            : period_len_(period_len) {
            learning_rate = std::sqrt((2.0f * std::log(2.0f)) / period_len);
            learning_rate_reset = std::clamp(learning_rate, 0.001f, 1.0f);
            learning_rate_curr = learning_rate;
            learning_rate_prev = 0.0f;
        }

        float operator*(float other) const {
            return learning_rate * other;
        }

        void update(size_t time) {
            if (time % period_len_ != 0) return;

            float hitrate_curr = static_cast<float>(hitrate) / period_len_;
            float hitrate_diff = hitrate_curr - hitrate_prev;

            float delta_LR = learning_rate_curr - learning_rate_prev;
            auto [delta, delta_HR] = updateInDeltaDirection(delta_LR, hitrate_diff);

            if (delta > 0) {
                learning_rate = std::min(learning_rate + std::abs(learning_rate * delta_LR), 1.0f);
                hitrate_zero_count = hitrate_nega_count = 0;
            } else if (delta < 0) {
                learning_rate = std::max(learning_rate - std::abs(learning_rate * delta_LR), 0.001f);
                hitrate_zero_count = hitrate_nega_count = 0;
            } else if (delta == 0 && hitrate_diff <= 0) {
                if (hitrate_curr <= 0 && hitrate_diff == 0) hitrate_zero_count++;
                if (hitrate_diff < 0) {
                    hitrate_nega_count++;
                    hitrate_zero_count++;
                }
                if (hitrate_zero_count >= 10 || hitrate_nega_count >= 10) {
                    learning_rate = learning_rate_reset;
                    hitrate_zero_count = hitrate_nega_count = 0;
                } else if (hitrate_diff < 0) {
                    updateInRandomDirection();
                }
            }

            learning_rate_prev = learning_rate_curr;
            learning_rate_curr = learning_rate;
            hitrate_prev = hitrate_curr;
            hitrate_diff_prev = hitrate_diff;
            hitrate = 0;
        }

    private:
        std::pair<int, int> updateInDeltaDirection(float lr_diff, float hr_diff) {
            float delta = lr_diff * hr_diff;
            int d = (delta != 0) ? (delta > 0 ? 1 : -1) : 0;
            int delta_HR = (d == 0 && lr_diff != 0) ? 0 : 1;
            return {d, delta_HR};
        }

        void updateInRandomDirection() {
            static std::mt19937 gen(123);
            static std::uniform_real_distribution<float> rand(0.0f, 1.0f);
            if (learning_rate >= 1.0f) learning_rate = 0.9f;
            else if (learning_rate <= 0.001f) learning_rate = 0.005f;
            else if (rand(gen) < 0.5f) learning_rate = std::min(learning_rate * 1.25f, 1.0f);
            else learning_rate = std::max(learning_rate * 0.75f, 0.001f);
        }
    };

    size_t time = 0;
    size_t cache_size;

    DequeDict<size_t, Entry> s;
    DequeDict<size_t, Entry> q;
    HeapDict<size_t, Entry> lfu;
    DequeDict<size_t, Entry> lru_hist;
    DequeDict<size_t, Entry> lfu_hist;

    float initial_weight = 0.5f;
    std::array<float, 2> W;
    AdaptiveLearningRate learning_rate;

    size_t q_limit;
    size_t s_limit;
    size_t q_size = 0;
    size_t s_size = 0;
    size_t dem_count = 0;
    size_t nor_count = 0;

    std::mt19937 rng{123};
    std::uniform_real_distribution<float> dist{0.0f, 1.0f};

public:
    explicit Cacheus_Policy(size_t cache_size_)
        : cache_size(cache_size_), learning_rate(cache_size_) {
        W = {initial_weight, 1 - initial_weight};
        q_limit = std::max<size_t>(1, static_cast<size_t>(0.01 * cache_size + 0.5));
        s_limit = cache_size - q_limit;
    }

    bool contains(size_t oblock) const {
        return s.contains(oblock) || q.contains(oblock);
    }

    void updateLearningRate() {
        learning_rate.update(time);
    }

    void updateWeights(float rewardLRU, float rewardLFU) {
        float r0 = rewardLRU * learning_rate.learning_rate;
        float r1 = rewardLFU * learning_rate.learning_rate;
        W[0] *= std::exp(r0);
        W[1] *= std::exp(r1);
        float sum = W[0] + W[1];
        W[0] /= sum;
        W[1] /= sum;

        if (W[0] >= 0.99f) W = {0.99f, 0.01f};
        else if (W[1] >= 0.99f) W = {0.01f, 0.99f};
    }

    int getEvictionChoice() {
        return dist(rng) < W[0] ? 0 : 1;
    }

    std::pair<bool, std::optional<size_t>> request(size_t oblock) {
        time++;

        bool is_miss = !contains(oblock);
        std::optional<size_t> evicted = std::nullopt;

        if (is_miss) {
            if (s_size + q_size >= cache_size) {
                int policy = getEvictionChoice();
                Entry lru_entry = q.first();
                Entry lfu_entry = lfu.min();
                Entry evict_entry = lru_entry;

                if (lru_entry.oblock_ == lfu_entry.oblock_) policy = -1;
                else if (policy == 1) evict_entry = lfu_entry;

                if (s.contains(evict_entry.oblock_)) {
                    s.erase(evict_entry.oblock_);
                    s_size--;
                } else if (q.contains(evict_entry.oblock_)) {
                    q.erase(evict_entry.oblock_);
                    q_size--;
                }
                if (evict_entry.is_demoted_) dem_count--;

                lfu.erase(evict_entry.oblock_);
                evicted = evict_entry.oblock_;
            }

            Entry new_entry(oblock, 1, time, true);
            q.set(oblock, new_entry);
            lfu.set(oblock, new_entry);
            q_size++;
        } else {
            Entry hit_entry = s.contains(oblock) ? s.get(oblock) : q.get(oblock);
            hit_entry.freq_++;
            hit_entry.time_ = time;
            lfu.set(oblock, hit_entry);
            if (s.contains(oblock)) {
                s.set(oblock, hit_entry);
            } else {
                q.erase(oblock);
                q_size--;
                if (s_size >= s_limit) {
                    Entry demoted = s.first();
                    s.erase(demoted.oblock_);
                    s_size--;
                    demoted.is_demoted_ = true;
                    dem_count++;
                    q.set(demoted.oblock_, demoted);
                    q_size++;
                }
                s.set(oblock, hit_entry);
                s_size++;
            }
            learning_rate.hitrate++;
        }

        updateLearningRate();
        return {is_miss, evicted};
    }
};

class Cacheus : public EvictionPolicy {
 public:
  explicit Cacheus(size_t capacity)
      : EvictionPolicy(capacity), policy_(Cacheus_Policy(capacity)) {}

  // Touch/insert a key. If inserting and over capacity, evict LRU and return
  // it.
  std::optional<K> request(K key) override {
    auto [_, evicted] = policy_.request(key);
    return evicted;
  }

 private:
    Cacheus_Policy policy_;
};