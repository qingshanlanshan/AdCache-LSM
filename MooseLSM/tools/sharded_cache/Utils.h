#pragma once

#include <memory>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <unordered_set>
#include <vector>
#include <optional>
#include <shared_mutex>
#include <atomic>

using K = size_t;
using V = std::string;

const int NUM_SHARDS = 64;
const int MAX_SCAN_LENGTH = 64;
const int RANGE_COUNT = 16;  // Number of ranges
const int WORKLOAD_DIM = RANGE_COUNT + 4;  // Workload input dimension.
// 2 params (a and b) for each range, 1 for point lookup, 1 for cache size
const int CACHE_PARAM_DIM = RANGE_COUNT * 2 + 2;  // Current cache parameters dimension.
// const int STATE_DIM = WORKLOAD_DIM + CACHE_PARAM_DIM;
const int STATE_DIM = WORKLOAD_DIM;
const int ACTION_DIM = CACHE_PARAM_DIM;  // We'll adjust the cache parameters continuously.
const int HIDDEN_DIM = 640;
const double ACTOR_LR = 1e-4;
const double CRITIC_LR = 1e-3;
const double GAMMA = 0.99;
const int NUM_STEPS = 1;
int n_updates = 0;


// updated by training thread
// 0～-3: range lookup param
// -2: point lookup param
// -1: block cache size
std::vector<float> cache_params_vector(CACHE_PARAM_DIM, 1.0);
std::vector<float> hit_rates;
std::shared_mutex vector_mutex;
std::map<K, size_t> str_to_param_idx;

// scan params: [a, b] = cache_params_vector[ret : ret + 2]
size_t params_scan_idx(std::string key_string) {
  K key = std::stoull(key_string);
  auto it = str_to_param_idx.upper_bound(key);
  size_t idx = RANGE_COUNT - 1;
  if (it != str_to_param_idx.end()) {
    idx = it->second;
  }
  return idx * 2;
}
size_t params_get_idx() { return cache_params_vector.size() - 2; }
size_t params_put_idx() { return cache_params_vector.size() - 1; }

struct TestStats {
  std::atomic<double> OP_time = 0;
  std::atomic<size_t> OP_count = 0;
  // get
  std::atomic<size_t> n_get = 0;
  std::atomic<size_t> n_hit = 0;
  std::atomic<double> get_time = 0;
  // scan
  std::atomic<size_t> n_scan = 0;
  std::atomic<double> scan_time = 0;
  std::atomic<size_t> scan_length = 0;
  // range cache
  std::atomic<size_t> length_in_cache = 0;
  std::atomic<double> time_in_cache = 0;
  std::atomic<size_t> n_in_cache = 0;
  std::atomic<size_t> length_in_db = 0;
  std::atomic<double> time_in_db = 0;
  std::atomic<size_t> n_in_db = 0;
  // put
  std::atomic<size_t> n_put = 0;
  std::atomic<double> put_time = 0;
  // block
  std::atomic<size_t> n_block_get = 0;
  std::atomic<size_t> n_block_hit = 0;
  std::atomic<size_t> n_block_miss = 0;

  void reset() {
    OP_time = 0;
    OP_count = 0;
    n_get = 0;
    n_hit = 0;
    get_time = 0;
    n_scan = 0;
    scan_time = 0;
    scan_length = 0;
    length_in_cache = 0;
    time_in_cache = 0;
    n_in_cache = 0;
    length_in_db = 0;
    time_in_db = 0;
    n_in_db = 0;
    n_put = 0;
    put_time = 0;
    n_block_get = 0;
    n_block_hit = 0;
  }

  double get_hitrate(int n_levels) {
    return 1 - 1.0 * n_block_miss /
                   (1 + n_get + scan_length / 4 + n_scan * (n_levels + 1));
  }
} stats;

struct LearningStats {
  size_t learning_window_size = (size_t)1e3;
  std::atomic<size_t> operation_count = 0;
  std::atomic<size_t> n_get = 0;
  std::atomic<size_t> n_scan = 0;
  std::atomic<size_t> n_put = 0;
  std::atomic<size_t> scan_len = 0;

  std::atomic<float> avg_scan_len = 0;
  std::atomic<size_t> max_scan_len = 0;
  std::atomic<int> freq[RANGE_COUNT] = {0};

  std::atomic<bool> warmup_done = false;

  float cache_to_db_ratio = 0.0;

  void get() {
    operation_count++;
    n_get++;
  }
  void put() {
    operation_count++;
    n_put++;
  }
  void scan(std::string key, size_t len) {
    operation_count++;
    scan_len += len;
    avg_scan_len = (avg_scan_len * n_scan + len) / (n_scan + 1);
    n_scan++;
    max_scan_len = std::max(max_scan_len.load(), len);
    size_t idx = params_scan_idx(key) / 2;
    freq[idx]++;
  }

  std::vector<float> ToVector() {
    std::vector<float> res;
    res.push_back((float)n_get / operation_count);
    res.push_back((float)n_scan / operation_count);
    res.push_back((float)n_put / operation_count);
    res.push_back((float)avg_scan_len / MAX_SCAN_LENGTH);
    res.push_back(cache_to_db_ratio);
    for (size_t i = 0; i < RANGE_COUNT; i++) {
      if (n_scan == 0)
        res.push_back(0);
      else
        res.push_back((float)freq[i] / n_scan);
    }
    return res;
  }

  void reset() {
    operation_count = 0;
    n_get = 0;
    n_scan = 0;
    n_put = 0;
    avg_scan_len = 0;
    max_scan_len = 0;
    std::fill(freq, freq + RANGE_COUNT, 0);
  }
} learning_stats;
