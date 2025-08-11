#include <gflags/gflags.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <numeric>
#include <queue>
#include <random>

#include "rocksdb/advanced_cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/monkey_filter.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "tools/data_structure.h"
#include "tools/operation_with_cache.h"
#include "tools/torch_train.h"
#include "util/string_util.h"

#include "cache/heap_cache.h"
#include <csignal>

using std::string;

DEFINE_string(level_capacities, "4194304,41943040,419430400,4194304000,41943040000,419430400000", "Comma-separated list of level capacities");
DEFINE_string(run_numbers, "1,1,1,1,1,1", "Comma-separated list of run numbers");
DEFINE_string(compaction_style, "default", "Compaction style");
DEFINE_int32(bpk, 10, "Bits per key for filter");
DEFINE_int32(kvsize, 1024, "Size of key-value pair");
DEFINE_string(workload, "prepare", "prepare or test");
DEFINE_string(path, "/tmp/db", "dbpath");
DEFINE_string(workload_file, "workload", "workload file");
DEFINE_int32(cache_size, 0, "Cache size");
DEFINE_int32(sst_size, 4194304, "SST size");
DEFINE_string(cache_style, "block", "Cache style: LRU, ELRU, heap, range, RLCache, block, lecar, cacheus");
DEFINE_int32(worker_threads_num, 0, "Number of worker threads");

// for learning
std::random_device rd;
std::mt19937 gen(rd());
size_t max_possible_scan_len = 64;
std::set<std::string> query_keys;
std::map<std::string, size_t> str_to_param_idx;
uint64_t db_size = 0;
auto launch_time = chrono::steady_clock::now();

inline std::string ItoaWithPadding(const uint64_t key, uint64_t size) {
  std::string key_str = std::to_string(key);
  std::string padding_str(size - key_str.size(), '0');
  key_str = padding_str + key_str;
  return key_str;
}

enum OperationType { GET, PUT, DELETE, SCAN };

struct Operation {
  OperationType type;
  std::string key;
  std::string value;
  size_t length;

  bool operator==(const Operation& op) const { return key == op.key; }
};

std::string random_value(const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

std::vector<Operation> get_operations_from_file(const std::string& file_path) {
  std::vector<Operation> operations;
  std::ifstream file(file_path);
  std::string line;
  std::lognormal_distribution<> dist;
  dist.param(std::lognormal_distribution<>::param_type(3, 0.5));
  // number of lines in the file
  size_t n_lines = 0;
  while (std::getline(file, line)) {
    n_lines++;
  }
  std::cout << "num queries: " << n_lines << std::endl;
  file.clear();
  file.seekg(0, std::ios::beg);

  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string op;
    iss >> op;
    Operation operation;
    if (op == "GET" || op == "READ") {
      operation.type = GET;
    } else if (op == "PUT" || op == "INSERT" || op == "UPDATE") {
      operation.type = PUT;
      operation.value = random_value(FLAGS_kvsize - 24);
    } else if (op == "DELETE") {
      operation.type = DELETE;
    } else if (op == "SCAN") {
      operation.type = SCAN;
    } else {
      std::cerr << "Unknown operation: " << op << std::endl;
      exit(1);
    }
    iss >> operation.key;
    if (operation.type == SCAN) {
      iss >> operation.length;
      if (operation.length > max_possible_scan_len)
        max_possible_scan_len = operation.length;
    }
    query_keys.insert(operation.key);
    operations.push_back(operation);
  }
  max_possible_scan_len *= 1.1;
  return operations;
}

void construct_cache_param_table() {
  std::vector<string> keys(query_keys.begin(), query_keys.end());
  size_t range_size = keys.size() / RANGE_COUNT;
  for (size_t i = 1; i < RANGE_COUNT; i++) {
    str_to_param_idx[keys[i * range_size]] = i - 1;
  }
}

size_t params_scan_idx(string key) {
  return 0;
  auto it = str_to_param_idx.upper_bound(key);
  size_t idx = RANGE_COUNT - 1;
  if (it != str_to_param_idx.end()) {
    idx = it->second;
  }
  return idx * 2;
}

size_t params_get_idx() { return cache_params_vector.size() - 2; }

size_t params_put_idx() { return cache_params_vector.size() - 1; }

void remove_dup_and_sort(std::vector<Operation>& ops) {
  std::sort(ops.begin(), ops.end(), [](const Operation& a, const Operation& b) {
    return a.key < b.key;
  });
  ops.erase(std::unique(ops.begin(), ops.end()), ops.end());
}

void PrepareDB_ingestion(rocksdb::DB* db, rocksdb::Options options) {
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));
  std::vector<Operation> ops = get_operations_from_file(FLAGS_workload_file);
  size_t total_size = 0;
  size_t level_size = FLAGS_sst_size;
  while (total_size < ops.size() * 1024) {
    options.level_capacities.push_back(level_size);
    total_size += level_size;
    level_size *= 10;
  }

  size_t ops_idx = 0;
  for (int i = options.level_capacities.size() - 1; i >= 0; i--) {
    if (ops_idx >= ops.size()) {
      break;
    }
    int n_entry_cur_level = options.level_capacities[i] / FLAGS_kvsize;
    std::vector<Operation> ops_cur_level;
    while (ops_idx < ops.size() &&
           (int)ops_cur_level.size() < n_entry_cur_level) {
      ops_cur_level.push_back(ops[ops_idx]);
      ops_idx++;
    }
    remove_dup_and_sort(ops_cur_level);

    for (size_t file_idx = 0; file_idx < ops_cur_level.size() / 4096 + 1;
         file_idx++) {
      std::string file = FLAGS_path + "/level_" + std::to_string(i) + "_" +
                         std::to_string(file_idx);
      rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
      writer.Open(file);
      for (size_t j = 0; j < 4096; j++) {
        if (file_idx * 4096 + j >= ops_cur_level.size()) break;
        writer.Put(ops_cur_level[file_idx * 4096 + j].key,
                   ops_cur_level[file_idx * 4096 + j].value);
      }
      writer.Finish();
      auto status =
          db->IngestExternalFile({file}, rocksdb::IngestExternalFileOptions());
    }
  }
}
// statistics for learning
struct LearningStatistics {
  size_t operation_count = 0;
  size_t n_get = 0;
  size_t n_scan = 0;
  size_t n_put = 0;
  size_t scan_len = 0;
  float last_timer = chrono::duration_cast<chrono::microseconds>(
                         chrono::steady_clock::now().time_since_epoch())
                         .count();

  float avg_scan_len = 0;
  size_t max_scan_len = 0;
  int freq[RANGE_COUNT] = {0};

  float timer() {
    auto now = chrono::duration_cast<chrono::microseconds>(
                   chrono::steady_clock::now().time_since_epoch())
                   .count();
    float res = now - last_timer;
    last_timer = now;
    return res;
  }

  void get() {
    operation_count++;
    n_get++;
  }
  void put() {
    operation_count++;
    n_put++;
  }
  void scan(string key, size_t len) {
    operation_count++;
    scan_len += len;
    avg_scan_len = (avg_scan_len * n_scan + len) / (n_scan + 1);
    n_scan++;
    max_scan_len = std::max(max_scan_len, len);
    size_t idx = params_scan_idx(key) / 2;
    freq[idx]++;
  }

  vector<float> ToVector() {
    vector<float> res;
    res.push_back((float)n_get / operation_count);
    res.push_back((float)n_scan / operation_count);
    res.push_back((float)n_put / operation_count);
    res.push_back((float)avg_scan_len / max_possible_scan_len);
    res.push_back((float)FLAGS_cache_size * 1024/ db_size);
    // for (size_t i = 0; i < RANGE_COUNT; i++) {
    //   if (n_scan == 0)
    //     res.push_back(0);
    //   else
    //     res.push_back((float)freq[i] / n_scan);
    // }
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
};

void dump_stats(TestStats& test_stats, std::ofstream& out, size_t n_levels, bool block_cache = false) {
  // std::ofstream out("/home/jiarui/CacheLSM/results", std::ios::app);
  if (test_stats.OP_count == 0) {
    test_stats.OP_count = 100000;
  }
  auto op_time = test_stats.OP_time / test_stats.OP_count;
  out << "OP time: " << op_time << std::endl;
  out << "hit rate: " << test_stats.get_hitrate(n_levels) << std::endl;
  out << "kv hit rate: " << (double)test_stats.n_hit / test_stats.n_get
      << std::endl;
  out << "scan hit rate: "
      << (double)test_stats.length_in_cache /
             (test_stats.length_in_cache + test_stats.length_in_db)
      << std::endl;
  out << "block hits: "
      << (double)test_stats.n_block_hit / (test_stats.n_scan * (n_levels + 1))
      << std::endl;
  out << "get time: " << (double)test_stats.get_time / test_stats.n_get
      << std::endl;
  out << "put time: " << (double)test_stats.put_time / test_stats.n_put
      << std::endl;
  out << "scan time: " << (double)test_stats.scan_time / test_stats.n_scan
      << std::endl;
  out << "db scan time: " << (double)test_stats.time_in_db / (test_stats.length_in_db + 1)
      << std::endl;
  out << "cache scan time: "
      << (double)test_stats.time_in_cache / (test_stats.length_in_cache + 1)
      << std::endl;
  // out.close();
}

void set_blockcache_capacity(rocksdb::Options options, float ratio) {
  auto blockcache = options.extern_options->block_cache;
  size_t size = ratio * FLAGS_cache_size * FLAGS_kvsize;
  blockcache->SetCapacity(size);
}

rocksdb::heap_cache::HeapCacheShard* extract_heapcacheshard(shared_ptr<rocksdb::Cache> block_cache, int i = 0)
{
  if (block_cache) {
    auto c = block_cache.get();
    string name = c->Name();
    if (name == "HeapCache")
    {
      auto heap_cache = dynamic_cast<rocksdb::heap_cache::HeapCache*>(c);
      return heap_cache->GetShard_idx(i);;
    }
  }
  return nullptr;
}

void set_cache_capacity(RangeCache* cache, float ratio) {
  cache->set_capacity((size_t)(ratio * FLAGS_cache_size)); 
}


void FileWorkload(rocksdb::DB* db, TestStats& stats, Cache* cache = nullptr, std::ofstream* out = nullptr, int file_idx = -1) {
  rocksdb::ReadOptions read_options;
  read_options.extern_options = new rocksdb::ExternOptions();
  read_options.extern_options->cache_style = FLAGS_cache_style;
  read_options.verify_checksums = false;

  size_t learning_window_size = (size_t)1e3;
  size_t print_window_size = (size_t)1e3;
  LearningStatistics learning_stats;
  bool warmup_done = false;

  if (FLAGS_cache_style == "RLCache") {
    // construct_cache_param_table();
    cache_params_vector[params_get_idx()] = 0.01;
    cache_params_vector[params_put_idx()] = 0.5;
  }

  struct {
    float get_time = 30;
    float put_time = 5;
  } baseline;
  unordered_map<string, size_t> key_freq_map;
  size_t total_freq = 0;
  size_t n_levels = db->GetOptions().num_levels;
  string filename = FLAGS_workload_file;
  if (file_idx >= 0) {
    filename += "_" + std::to_string(file_idx);
  }
  std::ifstream file(filename);
  std::string line;
  size_t num_operations = 0;
  while (std::getline(file, line)) {
    num_operations++;
  }
  std::cout << "num queries: " << num_operations << std::endl;
  file.clear();
  file.seekg(0, std::ios::beg);
  
  cout << "start testing" << endl;
  size_t idx = 0;
  while (std::getline(file, line)) {
    idx++;
    Operation operation;
    {
      std::istringstream iss(line);
      std::string op;
      iss >> op;
      if (op == "GET" || op == "READ") {
        operation.type = GET;
      } else if (op == "PUT" || op == "INSERT" || op == "UPDATE") {
        operation.type = PUT;
        operation.value = random_value(FLAGS_kvsize - 24);
      } else if (op == "DELETE") {
        operation.type = DELETE;
      } else if (op == "SCAN") {
        operation.type = SCAN;
      } else {
        std::cerr << "Unknown operation: " << op << std::endl;
        exit(1);
      }
      iss >> operation.key;
      if (operation.type == SCAN) {
        iss >> operation.length;
        if (operation.length > max_possible_scan_len)
          max_possible_scan_len = operation.length;
      }
    }
    auto op = operation;
    stats.OP_count++;
    if (idx % learning_window_size == 0) {
      auto statistics = db->GetOptions().statistics;
      auto hit_count =
          statistics->getAndResetTickerCount(rocksdb::BLOCK_CACHE_DATA_HIT);
      auto miss_count =
          statistics->getAndResetTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS);
      stats.n_block_get = hit_count + miss_count;
      stats.n_block_hit = hit_count;
      stats.n_block_miss = miss_count;
      // train and reset
      if (FLAGS_cache_style == "RLCache"){
        if (!warmup_done && cache) {
          // auto rl_cache = dynamic_cast<RLCache*>(cache);
          auto rl_cache = dynamic_cast<RangeCache*>(cache);
          if (!warmup_done && rl_cache->get_size() >= rl_cache->get_capacity() * 0.9) {
            warmup_done = true;
            learning_stats.timer();
          }
        }
        // train
        else if (warmup_done) {
          std::shared_lock<std::shared_mutex> lock(vector_mutex);
  #if 1
          float cache_ratio = cache_params_vector[params_put_idx()];
          auto rl_cache = dynamic_cast<RangeCache*>(cache);
          // gradually change the cache size to avoid too much eviction
          // cache_ratio = cache_ratio * 0.2 + 0.8 * rl_cache->get_capacity() / FLAGS_cache_size; 
          if (learning_stats.n_scan == 0) 
          {
            cache_ratio = (float) rl_cache->get_capacity() / FLAGS_cache_size;
            cache_ratio = max((float)0.1, cache_ratio);
            if (rl_cache->get_size() >= rl_cache->get_capacity() * 0.9)
              cache_ratio = min(1.0, cache_ratio + 0.1);
          }
          else
          {
            cache_ratio *= 0.94;
          }
          // if (cache_ratio < 0.02)
          //   cache_ratio = 0;
          // else if (cache_ratio > 0.08)
          //   cache_ratio = 1;
          float cur_blockcache_ratio = 1 - cache_ratio;
          set_cache_capacity(rl_cache, cache_ratio);
          set_blockcache_capacity(db->GetOptions(), cur_blockcache_ratio);
          cache_params_vector[params_put_idx()] = cache_ratio;
          cout << "blockcache ratio: " << cur_blockcache_ratio << endl;
          *out << "blockcache ratio: " << cur_blockcache_ratio << std::endl;
  #endif
          workload_vector = learning_stats.ToVector();
          // hit rate
          cur_hitrate = stats.get_hitrate(n_levels);
          sem.release();
        }
        // reset
        learning_stats.reset();
      }
      
    }
    if (idx % print_window_size == 0 && out) {
      auto statistics = db->GetOptions().statistics;
      auto miss_count =
          statistics->getAndResetTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS);
      if (FLAGS_cache_style == "heap") {
        auto options = db->GetOptions();
        auto heapcache = extract_heapcacheshard(options.extern_options->block_cache);
        auto heapcache_stats = heapcache->stats_;
        // *out << "hit rate: " << heapcache->get_hit_rate() << std::endl;
        *out << "hit rate: " << 1 - (float)miss_count / (heapcache_stats.n_block_get + heapcache_stats.n_kv_get)
              << std::endl;
        *out << "kv hit rate: "
              << (float)heapcache_stats.n_kv_hit / heapcache_stats.n_kv_get
              << std::endl;
        *out << "block hit rate: "
              << (float)heapcache_stats.n_block_hit /
                    heapcache_stats.n_block_get
              << std::endl;
        heapcache->reset_stats();

        auto op_time = stats.OP_time / stats.OP_count;
        *out << "OP time: " << op_time << std::endl;
      } else {
        dump_stats(stats, *out, n_levels, FLAGS_cache_style == "block");
        *out << "counter: " << counter << std::endl;
      }
      stats.reset();
    }
    auto start = std::chrono::steady_clock::now();
    if (op.type == GET) {
      stats.n_get++;
      std::string value;
      auto s = chrono::steady_clock::now();
      rocksdb::Status status = rocksdb::Status::OK();
      if (FLAGS_cache_style != "RLCache") {
        status = get_with_cache(db, cache, op.key, value, stats);
      } else {
        auto rl_cache = dynamic_cast<RLCache*>(cache);
        if (rl_cache->get(op.key, value)) {
          stats.n_hit++;
        } else {
          status = db->Get(read_options, op.key, &value);
          float threshold = cache_params_vector[params_get_idx()];  
          rl_cache->put(op.key, value, threshold);
        }
      }
      auto e = chrono::steady_clock::now();
      stats.get_time +=
          chrono::duration_cast<chrono::microseconds>(e - s).count();
      learning_stats.get();

      if (!status.ok()) {
        std::cerr << "Failed to get key " << op.key 
                  << " status:" << status.ToString() << std::endl;
        exit(1);
      }
    } else if (op.type == PUT) {
      stats.n_put++;
      auto s = chrono::steady_clock::now();
      auto status = put_with_cache(db, cache, op.key, op.value, stats);
      auto e = chrono::steady_clock::now();
      stats.put_time +=
          chrono::duration_cast<chrono::microseconds>(e - s).count();
      learning_stats.put();
      if (!status.ok()) {
        std::cerr << "Failed to put key " << op.key
                  << " status:" << status.ToString() << std::endl;
        exit(1);
      }
    } else if (op.type == DELETE) {
      auto status = delete_with_cache(db, cache, op.key, stats);
      if (!status.ok()) {
        std::cerr << "Failed to delete key " << op.key 
                  << " status:" << status.ToString() << std::endl;
        exit(1);
      }
    } else if (op.type == SCAN) {
      stats.n_scan++;
      stats.scan_length += op.length;
      learning_stats.scan(op.key, op.length);
      auto s = chrono::steady_clock::now();
      if (FLAGS_cache_style != "RLCache") {
        scan_with_cache(db, cache, op.key, op.length, stats);
      } else {
        size_t cache_length = max_possible_scan_len;
        if (warmup_done) {
          auto i = params_scan_idx(op.key);
          float a = cache_params_vector[i];
          float b = cache_params_vector[i + 1];
          
          if (a < 0 || a > 1 || b < 0 || b > 1) {
            std::cerr << "Invalid cache params: ";
            for (auto& it : cache_params_vector) {
              std::cerr << it << " ";
            }
            std::cerr << std::endl;
            exit(1);
          }
          a *= max_possible_scan_len;
          a = max((float)16, a);
          if (op.length > a) {
            cache_length -= a;
            cache_length *= b; 
            cache_length += a;
          }
        }
        auto range_cache = dynamic_cast<RangeCache*>(cache);
        // if (range_cache->get_capacity() < 0.1 * FLAGS_cache_size) {
        //   cache_length = 0;
        // }
        range_cache_scan_with_len(db, read_options, range_cache, op.key, op.length, cache_length, stats);
      }
      auto e = chrono::steady_clock::now();
      stats.scan_time +=
          chrono::duration_cast<chrono::microseconds>(e - s).count();
    }
    auto end = std::chrono::steady_clock::now();
    stats.OP_time +=
        std::chrono::duration_cast<std::chrono::microseconds>(end - start)
            .count();
    if (idx % max(1, (int)(num_operations / 10)) == 0)
    {
      auto time_since_launch = chrono::duration_cast<chrono::seconds>(end - launch_time).count();
      // human readable time
      std::cout << "Time since launch: " << time_since_launch / 3600 << "h "
                << (time_since_launch % 3600) / 60 << "m "
                << time_since_launch % 60 << "s \t";
      std::cout << "Progress: " << (idx * 100) / num_operations << "%\r";
      std::cout.flush();
    }
  }
  delete read_options.extern_options;
}

void set_table_options(rocksdb::Options& options) {
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));
  options.extern_options = new rocksdb::ExternOptions();
  options.extern_options->cache_style = FLAGS_cache_style;
  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  table_options->checksum = rocksdb::kNoChecksum;
  if (FLAGS_cache_style == "block") {
    auto block_cache = rocksdb::NewLRUCache((size_t)FLAGS_cache_size * FLAGS_kvsize);
    table_options->block_cache = block_cache;
    options.extern_options->block_cache = block_cache;
  } else if (FLAGS_cache_style == "heap") {
    auto block_cache =
        rocksdb::NewHeapCache((size_t)FLAGS_cache_size * FLAGS_kvsize);
    table_options->block_cache = block_cache;
    options.extern_options->block_cache = block_cache;
  }
  else if (FLAGS_cache_style == "RLCache")
  {
    auto block_cache = rocksdb::NewLRUCache((size_t)FLAGS_cache_size * FLAGS_kvsize / 2, 0);
    // auto block_cache = rocksdb::NewHeapCache((size_t)FLAGS_cache_size * FLAGS_kvsize / 2, 0);
    table_options->block_cache = block_cache;
    options.extern_options->block_cache = block_cache;
  }
  else {
    table_options->block_cache = rocksdb::NewLRUCache(0);
    options.extern_options->block_cache = table_options->block_cache;
  }
  auto block_cache = options.extern_options->block_cache;
  cout << "block cache size: " << block_cache->GetCapacity() << endl;
}

rocksdb::Options get_default_options() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4 << 20;
  options.level0_file_num_compaction_trigger = 4;
  options.level_compaction_dynamic_level_bytes = false;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 8;
  options.max_bytes_for_level_base =
      options.max_bytes_for_level_base * options.max_bytes_for_level_multiplier;
  set_table_options(options);
  return options;
}

rocksdb::Options get_moose_options() {
  rocksdb::Options options;
  options.compaction_style = rocksdb::kCompactionStyleMoose;
  options.create_if_missing = true;
  options.write_buffer_size = 4 << 20;
  options.level_compaction_dynamic_level_bytes = false;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 8;

  std::vector<std::string> split_st =
      rocksdb::StringSplit(FLAGS_level_capacities, ',');
  std::vector<uint64_t> level_capacities;
  for (auto& s : split_st) {
    level_capacities.push_back(std::stoull(s));
  }
  split_st = rocksdb::StringSplit(FLAGS_run_numbers, ',');
  std::vector<int> run_numbers;
  for (auto& s : split_st) {
    run_numbers.push_back(std::stoi(s));
  }
  std::vector<uint64_t> physical_level_capacities;
  for (int i = 0; i < (int)run_numbers.size(); i++) {
    uint64_t run_size = level_capacities[i] / run_numbers[i];
    for (int j = 0; j < (int)run_numbers[i]; j++) {
      physical_level_capacities.push_back(run_size);
    }
  }
  options.level_capacities = physical_level_capacities;
  options.run_numbers = run_numbers;
  options.num_levels =
      std::accumulate(run_numbers.begin(), run_numbers.end(), 0);

  set_table_options(options);  
  return options;
}

void start_test_workers(rocksdb::DB* db, TestStats& stats, Cache* cache = nullptr, std::ofstream* out = nullptr) {
  std::cout << "Starting test workers with " << FLAGS_worker_threads_num
            << " threads." << std::endl;
  std::vector<std::thread> threads;
  for (int i = 1; i < FLAGS_worker_threads_num; i++) {
    threads.emplace_back(FileWorkload, db, std::ref(stats), cache, out, i);
  }
  FileWorkload(db, stats, cache, out, 0);
  for (auto& t : threads) {
    t.join();
  }
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::ofstream out("/home/jiarui/AdCache/results", std::ios::app);
  out << "cache_style: " << FLAGS_cache_style << std::endl;
  out << "bpk: " << FLAGS_bpk << std::endl;
  out << "cache_size: " << FLAGS_cache_size << std::endl;

  rocksdb::DB* db;
  rocksdb::Options options;
  if (FLAGS_compaction_style == "moose") {
    options = get_moose_options();
  } else {
    options = get_default_options();
  }

  if (FLAGS_workload == "test") {
    // options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
  }
  options.statistics = rocksdb::CreateDBStatistics();
  options.advise_random_on_open = false;
  options.max_open_files = -1;
  options.compression = rocksdb::kNoCompression;

  auto status = rocksdb::DB::Open(options, FLAGS_path, &db);
  if (!status.ok()) {
    std::cerr << "Failed to open db: " << status.ToString() << std::endl;
    return 1;
  }
  // estimate db size
  db->GetIntProperty(rocksdb::DB::Properties::kTotalSstFilesSize, &db_size);
  Cache* cache = nullptr;
  int cache_capacity = 0;

  if (FLAGS_workload == "test" && FLAGS_cache_size > 0) {
    cache_capacity = FLAGS_cache_size;
  }
  std::cout << "cache_capacity: " << cache_capacity << " entries" << std::endl;
  std::cout << "cache style: " << FLAGS_cache_style << std::endl;
  if (cache_capacity > 0) {
    if (FLAGS_cache_style == "LRU") {
      cache = new LRUCache(cache_capacity);
    } else if (FLAGS_cache_style == "ELRU") {
      cache = new ELRU(cache_capacity, 20);
    }
    else if (FLAGS_cache_style == "range") {
      cache = new RangeCache(cache_capacity);
    } else if (FLAGS_cache_style == "lecar") {
      cache = new RangeCache_LeCaR(cache_capacity);
    } else if (FLAGS_cache_style == "RLCache") {
      // cache = new RLCache(cache_capacity * 15 / 16);
      cache = new RLCache(cache_capacity / 2);
    } else if (FLAGS_cache_style == "block" || FLAGS_cache_style == "heap") {
      cache = nullptr;
    } else if (FLAGS_cache_style == "cacheus") {
      cache = new RangeCache_Cacheus(cache_capacity);
    } else {
      std::cerr << "Unknown cache style: " << FLAGS_cache_style << std::endl;
      return 1;
    }
    if (cache) std::cout << "cache name: " << cache->name() << std::endl;
  }

  TestStats test_stats;
  if (FLAGS_workload == "prepare") {
    FileWorkload(db, test_stats, nullptr, &out);
    // PrepareDB_ingestion(db, options);
  } else if (FLAGS_workload == "test") {
    if (FLAGS_cache_style == "RLCache") {
      std::thread trainworker(train_worker_function);
      // FileWorkload(db, test_stats, cache, &out);
      start_test_workers(db, test_stats, cache, &out);
      exit_train_worker.store(true);
      sem.release();  // Release in case the worker is waiting.
      trainworker.join();
    } else {
      // FileWorkload(db, test_stats, cache, &out);
      start_test_workers(db, test_stats, cache, &out);
    }
  }
  std::string stat;
  db->GetProperty("rocksdb.stats", &stat);
  std::cout << stat << std::endl;
  std::cout << "statistics: " << options.statistics->ToString() << std::endl;
  std::cout << counter << std::endl;
  if (FLAGS_workload == "test") {
    out << "cache size: " << (cache ? cache->get_capacity() : 0) << std::endl;
    out << "block cache size: " << options.extern_options->block_cache->GetCapacity()
        << std::endl;
    out << "block cache ratio: " << (float) options.extern_options->block_cache->GetCapacity() / FLAGS_kvsize / FLAGS_cache_size << endl;
    auto full_positive =
        options.statistics->getTickerCount(rocksdb::BLOOM_FILTER_FULL_POSITIVE);
    auto usefull =
        options.statistics->getTickerCount(rocksdb::BLOOM_FILTER_USEFUL);
    auto full_true_positive = options.statistics->getTickerCount(
        rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE);
    out << "FPR: "
        << (double)(full_positive - full_true_positive) /
               (full_positive - full_true_positive + usefull)
        << std::endl;
    rocksdb::HistogramData hist;
    options.statistics->histogramData(rocksdb::DB_GET, &hist);
    auto db_get_time = hist.average;
    out << "db get time: " << db_get_time << std::endl;
    options.statistics->histogramData(rocksdb::SST_READ_MICROS, &hist);
    out << "sst read time: " << hist.average << std::endl;
    out << "sst read count: " << hist.count << std::endl;
  }
  out.close();

  auto block_cache =
      options.extern_options ? options.extern_options->block_cache : nullptr;
  // if (block_cache) {
  //   auto c = block_cache.get();
  //   string name = c->Name();
  //   if (name == "HeapCache") c->Lookup("report");
  // }
  auto heapcacheshard = extract_heapcacheshard(block_cache);
  if (heapcacheshard) heapcacheshard->report();
  db->Close();
  delete db;
  if (cache) delete cache;
  return 0;
}