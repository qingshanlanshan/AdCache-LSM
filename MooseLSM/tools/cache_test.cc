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
#include "tools/sharded_cache/ShardedCacheBuilder.h"
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
DEFINE_string(cache_style, "block", "Cache style: kv, range, adcache, block, lecar, cacheus");
DEFINE_int32(worker_threads_num, 1, "Number of worker threads");

// for learning
std::random_device rd;
std::mt19937 gen(rd());
size_t max_possible_scan_len = 64;
K min_key = 0;
K max_key = 1e8;
size_t num_levels = 0;
auto launch_time = chrono::steady_clock::now();

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
    operations.push_back(operation);
  }
  max_possible_scan_len *= 1.1;
  return operations;
}

void construct_cache_param_table() {
  size_t range_size = (max_key - min_key) / RANGE_COUNT;
  for (size_t i = 1; i < RANGE_COUNT; i++) {
    str_to_param_idx[i * range_size + min_key] = i - 1;
  }
}

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

int get_level_number(rocksdb::DB* db) {
  std::string stat;
  int level_number = 0;
  for (int i = 1; i < db->NumberLevels(); i++) {
    db->GetProperty("rocksdb.num-files-at-level" + std::to_string(i), &stat);
    if (stat != "0") level_number++;
  }
  return level_number;
}

float dump_stats(std::ofstream& out, bool block_cache = false) {
  auto hit_rate = stats.get_hitrate(num_levels);
  auto op_time = stats.OP_time / stats.OP_count;
  out << "OP time: " << op_time << std::endl;
  out << "hit rate: " << hit_rate << std::endl;
  out << "kv hit rate: " << (double)stats.n_hit / stats.n_get
      << std::endl;
  out << "scan hit rate: "
      << (double)stats.length_in_cache /
             (stats.length_in_cache + stats.length_in_db)
      << std::endl;
  // out << "block hits: "
  //     << (double)stats.n_block_hit / (stats.n_scan * (n_levels + 1))
  //     << std::endl;
  out << "get time: " << (double)stats.get_time / stats.n_get
      << std::endl;
  out << "put time: " << (double)stats.put_time / stats.n_put
      << std::endl;
  out << "scan time: " << (double)stats.scan_time / stats.n_scan
      << std::endl;
  out << "db scan time: " << (double)stats.time_in_db / (stats.length_in_db + 1)
      << std::endl;
  out << "cache scan time: "
      << (double)stats.time_in_cache / (stats.length_in_cache + 1)
      << std::endl;
  return hit_rate;
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

void FileWorkload(rocksdb::DB* db, std::optional<ShardedCacheBase>& cache, std::ofstream* out = nullptr, int file_idx = -1) {
  rocksdb::ReadOptions read_options;
  read_options.extern_options = new rocksdb::ExternOptions();
  read_options.extern_options->cache_style = FLAGS_cache_style;
  read_options.verify_checksums = false;

  size_t print_window_size = (size_t)1e3;
  bool warmup_done = false;

  if (FLAGS_cache_style == "adcache") {
    construct_cache_param_table();
    cache_params_vector[params_get_idx()] = 0.01;
    cache_params_vector[params_put_idx()] = 0.5;
  }

  struct {
    float get_time = 30;
    float put_time = 5;
  } baseline;
  unordered_map<string, size_t> key_freq_map;
  size_t total_freq = 0;
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
  file.clear();
  file.seekg(0, std::ios::beg);
  
  cout << "Workload starts with " << num_operations << " operations from file " << filename << endl;
  size_t idx = 0;
  while (std::getline(file, line)) {
    idx++;
    Operation op;
    // read operation from line
    {
      std::istringstream iss(line);
      std::string op_str;
      iss >> op_str;
      if (op_str == "GET" || op_str == "READ") {
        op.type = GET;
      } else if (op_str == "PUT" || op_str == "INSERT" || op_str == "UPDATE") {
        op.type = PUT;
        op.value = random_value(FLAGS_kvsize - 24);
      } else if (op_str == "DELETE") {
        op.type = DELETE;
      } else if (op_str == "SCAN") {
        op.type = SCAN;
      } else {
        std::cerr << "Unknown operation: " << op_str << std::endl;
        exit(1);
      }
      iss >> op.key;
      if (op.type == SCAN) {
        iss >> op.length;
        if (op.length > max_possible_scan_len)
          max_possible_scan_len = op.length;
      }
    }

    if (learning_stats.operation_count > learning_stats.learning_window_size)
      // inform learning worker
      sem.trigger();
    

    if (idx % print_window_size == 0 && out && file_idx <= 0) {
      auto statistics = db->GetOptions().statistics;
      auto hit_count = statistics->getAndResetTickerCount(rocksdb::BLOCK_CACHE_DATA_HIT);
      auto miss_count = statistics->getAndResetTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS);
      // since the number of levels changes over time, update during the run
      num_levels = get_level_number(db);
      stats.n_block_get = hit_count + miss_count;
      stats.n_block_hit = hit_count;
      stats.n_block_miss = miss_count;
      auto hit_rate = dump_stats(*out, FLAGS_cache_style == "block");
      {
        std::unique_lock<std::shared_mutex> lock(vector_mutex);
        hit_rates.push_back(hit_rate);
      }
      stats.reset();
    }
    if (op.type == GET) {
      float threshold = 0.0;
      if (FLAGS_cache_style == "adcache") {
        std::shared_lock<std::shared_mutex> lock(vector_mutex);
        threshold = cache_params_vector[params_get_idx()];
      }
      auto status = get_with_cache(db, cache, op.key, threshold);
      learning_stats.get();

      if (!status.ok()) {
        std::cerr << "Failed to get key " << op.key 
                  << " status:" << status.ToString() << std::endl;
        exit(1);
      }
    } else if (op.type == PUT) {
      auto status = put_with_cache(db, cache, op.key, op.value);
      learning_stats.put();

      if (!status.ok()) {
        std::cerr << "Failed to put key " << op.key
                  << " status:" << status.ToString() << std::endl;
        exit(1);
      }
    } else if (op.type == DELETE) {
      throw std::runtime_error("Delete operation is not implemented yet.");
    } else if (op.type == SCAN) {
      size_t cache_length = op.length;
      if (FLAGS_cache_style == "adcache" && ENABLE_ADMISSION_CONTROL) {
        std::shared_lock<std::shared_mutex> lock(vector_mutex);
        auto i = params_scan_idx(op.key);
        auto a = cache_params_vector[i];
        auto b = cache_params_vector[i + 1];
        std::clamp(a, 0.0f, 1.0f);
        std::clamp(b, 0.0f, 1.0f);
        a *= max_possible_scan_len;
        a = std::max((float)16, a);
        if (op.length > a) {
          cache_length -= a;
          cache_length *= b; 
          cache_length += a;
        }
      }

      scan_with_cache(db, cache, op.key, op.length, cache_length, read_options);
      learning_stats.scan(op.key, op.length);
    }

    if (idx % max(1, (int)(num_operations / 10)) == 0)
    {
      // current date and time
      auto current_date = chrono::system_clock::now();
      auto current_time = chrono::system_clock::to_time_t(current_date);
      std::cout << std::ctime(&current_time);
      // time since launch
      auto time_since_launch = chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - launch_time).count();
      // human readable time
      std::cout << "Progress: " << (idx * 100) / num_operations << "% "
                << "Time since launch: " << time_since_launch / 3600 << "h "
                << (time_since_launch % 3600) / 60 << "m "
                << time_since_launch % 60 << "s " << std::endl;
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
  size_t shard_bits = std::log2(NUM_SHARDS);
  if (FLAGS_cache_style == "block") {
    auto block_cache = rocksdb::NewLRUCache((size_t)FLAGS_cache_size * FLAGS_kvsize, shard_bits);
    table_options->block_cache = block_cache;
    options.extern_options->block_cache = block_cache;
  }
  else if (FLAGS_cache_style == "adcache"&&false)
  {
    auto block_cache = rocksdb::NewLRUCache((size_t)FLAGS_cache_size * FLAGS_kvsize / 2, shard_bits);
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

void start_test_workers(rocksdb::DB* db, std::optional<ShardedCacheBase>& cache, std::ofstream* out = nullptr) {
  std::cout << "Starting test workers with " << FLAGS_worker_threads_num
            << " threads." << std::endl;
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_worker_threads_num; i++) {
    threads.emplace_back(FileWorkload, db, std::ref(cache), out, i);
  }

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

  if (FLAGS_workload == "test" && ENABLE_DIRECTIO) {
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

  int cache_capacity = 0;
  if (FLAGS_workload == "test" && FLAGS_cache_size > 0) {
    cache_capacity = FLAGS_cache_size;
    // if (FLAGS_cache_style == "adcache") 
    //   cache_capacity /= 2;
  }
  std::cout << "Cache params:\n"
            << "\tmin_key: " << min_key << "\n"
            << "\tmax_key: " << max_key << "\n"
            << "\tcache_capacity: " << cache_capacity << "\n"
            << "\tnum_shards: " << NUM_SHARDS << "\n"
            << "\tcache_style: " << FLAGS_cache_style << std::endl;
  auto cache = ShardedCacheBuilder::BuildShardedCache(
    (K)min_key,
    (K)max_key,
    cache_capacity,
    NUM_SHARDS,
    FLAGS_cache_style
  );
  if (cache) 
    std::cout << "cache name: " << cache.value().name << std::endl;
  

  if (FLAGS_workload == "prepare") {
    FileWorkload(db, std::ref(cache), &out);
    // PrepareDB_ingestion(db, options);
  } else if (FLAGS_workload == "test") {
    if (FLAGS_cache_style == "adcache") {
      learning_stats.cache_to_db_ratio = (float)cache_capacity / (max_key - min_key);
      std::thread trainworker(train_worker_function, db, FLAGS_cache_size, FLAGS_kvsize, std::ref(cache), &out);
      start_test_workers(db, std::ref(cache), &out);
      exit_train_worker.store(true);
      sem.trigger();  // Release in case the worker is waiting.
      trainworker.join();
    } else {
      start_test_workers(db, std::ref(cache), &out);
    }
  }
  std::string stat;
  db->GetProperty("rocksdb.stats", &stat);
  std::cout << stat << std::endl;
  std::cout << "statistics: " << options.statistics->ToString() << std::endl;
  if (FLAGS_workload == "test") {
    out << "cache size: " << (cache ? cache->get_capacity(): 0) << std::endl;
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
    out << "db get time: " << hist.average << std::endl;
    options.statistics->histogramData(rocksdb::SST_READ_MICROS, &hist);
    out << "sst read time: " << hist.average << std::endl;
    out << "sst read count: " << hist.count << std::endl;
  }
  out.close();

  auto block_cache = options.extern_options ? options.extern_options->block_cache : nullptr;
  auto heapcacheshard = extract_heapcacheshard(block_cache);
  if (heapcacheshard) heapcacheshard->report();
  db->Close();
  delete db;

  return 0;
}