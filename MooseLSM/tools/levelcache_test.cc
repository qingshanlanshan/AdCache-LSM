#include <iostream>
#include <numeric>
#include <gflags/gflags.h>
#include <random>
#include <fstream>
#include <chrono>
#include <algorithm>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/monkey_filter.h"
#include "util/string_util.h"
#include "rocksdb/monkey_filter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/advanced_cache.h"
#include "tools/data_structure.h"
#include "tools/operation_with_cache.h"

using std::string;

DEFINE_int32(bpk, 5, "Bits per key for filter");
DEFINE_int32(kvsize, 1024, "Size of key-value pair");
DEFINE_string(workload, "prepare", "prepare or test");
DEFINE_string(path, "/tmp/db", "dbpath");
DEFINE_string(workload_file, "workload", "workload file");
DEFINE_int32(cache_size, 0, "Cache size");
DEFINE_int32(sst_size, 4194304, "SST size");

size_t actual_filter_size = 0;
std::vector<double> monkey_bpks;
CountMinSketch cmsketch(4, 256);

inline std::string ItoaWithPadding(const uint64_t key, uint64_t size) {
  std::string key_str = std::to_string(key);
  std::string padding_str(size - key_str.size(), '0');
  key_str = padding_str + key_str;
  return key_str;
}

enum OperationType
{
  GET,
  PUT,
  DELETE,
  SCAN
};

struct Operation
{
  OperationType type;
  std::string key;
  std::string value;

  bool operator==(const Operation& op) const
  {
    return key == op.key;
  }
};

class KeyGenerator {
 public:
  KeyGenerator(uint64_t start, uint64_t end, uint64_t key_size,
               uint64_t value_size, bool shuffle = true) {
    start_ = start;
    end_ = end;
    idx_ = 0;
    key_size_ = key_size;
    value_size_ = value_size;
    shuffle_ = shuffle;
  }
  std::string Key() const { return ItoaWithPadding(keys_[idx_], key_size_); }
  std::string Value() const {
    return ItoaWithPadding(keys_[idx_], value_size_);
  }
  bool Next() {
    idx_++;
    return idx_ < keys_.size();
  }
  void SeekToFirst() {
    for (uint64_t i = start_; i < end_; i++) {
      keys_.push_back(i);
    }
    if (shuffle_) {
      auto rng = std::default_random_engine{};
      std::shuffle(std::begin(keys_), std::end(keys_), rng);
    }
    idx_ = 0;
  }

 private:
  uint64_t idx_;
  std::vector<uint64_t> keys_;
  uint64_t start_;
  uint64_t end_;
  uint64_t key_size_;
  uint64_t value_size_;
  bool shuffle_;
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
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string op;
    iss >> op;
    Operation operation;
    if (op == "GET" || op == "READ") {
      operation.type = GET;
    } else if (op == "PUT" || op == "INSERT" || op == "UPDATE") {
      operation.type = PUT;
    } else if (op == "DELETE") {
      operation.type = DELETE;
    } else if (op == "SCAN") {
      operation.type = SCAN;
    } else {
      std::cerr << "Unknown operation: " << op << std::endl;
      exit(1);
    }
    iss >> operation.key;
    if (operation.type == PUT) {
      operation.value = random_value(FLAGS_kvsize - 24);
    }
    operations.push_back(operation);
  }
  return operations;
}

void remove_dup_and_sort(std::vector<Operation>& ops) {
  std::sort(ops.begin(), ops.end(), [](const Operation& a, const Operation& b) { return a.key < b.key; } );
  ops.erase(std::unique( ops.begin(), ops.end()), ops.end());
}

void PrepareDB_ingestion(rocksdb::DB* db, rocksdb::Options options) {
  std::vector<Operation> ops = get_operations_from_file(FLAGS_workload_file+"_datset.dat");
  size_t total_size = 0;
  size_t level_size = FLAGS_sst_size;
  while (total_size < ops.size() * 1024)
  {
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
    while (ops_idx < ops.size() && (int)ops_cur_level.size() < n_entry_cur_level) {
      ops_cur_level.push_back(ops[ops_idx]);
      ops_idx ++;
    }
    remove_dup_and_sort(ops_cur_level);
    std::string file = FLAGS_path + "/level_" + std::to_string(i);
    auto table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
    table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    writer.Open(file);
    for (auto& op : ops_cur_level) {
      writer.Put(op.key, op.value);
    }
    writer.Finish();
    auto status = db->IngestExternalFile({file}, rocksdb::IngestExternalFileOptions());
  }
}

void FileWorkload(rocksdb::DB* db, TestStats& stats, rocksdb::ReadOptions* read_options = nullptr)
{
  if (read_options == nullptr)
  {
    read_options = new rocksdb::ReadOptions();
  }
  Cache* cache = nullptr;
  string file_name;
  if (FLAGS_workload == "test")
  {
    file_name = FLAGS_workload_file+"_query.dat";
  }
  else
  {
    file_name = FLAGS_workload_file+"_datset.dat";
  }
  auto operations = get_operations_from_file(file_name);
  auto start = std::chrono::steady_clock::now();
  for (size_t idx = 0; idx < operations.size(); ++idx)
  {
    auto op = operations[idx];
    // float scan_ratio = 0.5;
    // if (op.type == SCAN || op.type == GET)
    // {
    //   if (rand() % 100 < scan_ratio * 100)
    //   {
    //     op.type = SCAN;
    //   }
    //   else
    //   {
    //     op.type = GET;
    //   }
    // }
    if (FLAGS_workload == "prepare" && idx % (operations.size() / 10) == 0)
    {
      std::cout << "progress: " << idx << "/" << operations.size() << std::endl;
    }
    if (op.type == GET)
    {
      stats.n_get ++;
      std::string value;
      auto s = chrono::steady_clock::now();
      // auto status = get_with_cache(db, cache, op.key, value, stats);
      auto status = db->Get(*read_options, op.key, &value);
      auto e = chrono::steady_clock::now();
      stats.get_time += chrono::duration_cast<chrono::microseconds>(e - s).count();
      if (!status.ok())
      {
        std::cerr << "Failed to get key " << op.key << std::endl;
        exit(1);
      }
    }
    else if (op.type == PUT)
    {
      stats.n_put ++;
      auto s = chrono::steady_clock::now();
      // auto status = put_with_cache(db, cache, op.key, op.value, stats);
      auto status = db->Put(rocksdb::WriteOptions(), op.key, op.value);
      auto e = chrono::steady_clock::now();
      stats.put_time += chrono::duration_cast<chrono::microseconds>(e - s).count();
      if (!status.ok())
      {
        std::cerr << "Failed to put key " << op.key << " value " << op.value << std::endl;
        exit(1);
      }
    }
    else if (op.type == DELETE)
    {
      // auto status = delete_with_cache(db, cache, op.key, stats);
      auto status = db->Delete(rocksdb::WriteOptions(), op.key);
      if (!status.ok())
      {
        std::cerr << "Failed to delete key " << op.key << std::endl;
        exit(1);
      }
    }
    else if (op.type == SCAN)
    {
      stats.n_scan ++;
      int scan_length = 16;
      // int scan_length = rand() % 91 + 10;
      auto s = chrono::steady_clock::now();
      // auto status = scan_with_cache(db, cache, op.key, scan_length, res, stats);
      
      auto iter = db->NewIterator(*read_options);
      iter->Seek(op.key);
      for (int i = 0; i < scan_length && iter->Valid(); i++)
      {
        // cout << iter->key().ToString() << " " << iter->value().ToString() << endl;
        iter->Next();
      }
      delete iter;
      // cout << endl;
      auto e = chrono::steady_clock::now();
      stats.scan_time += chrono::duration_cast<chrono::microseconds>(e - s).count();
    }
  }
  auto end = std::chrono::steady_clock::now();
  stats.OP_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  stats.OP_count = operations.size();
}


rocksdb::Options get_default_options() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4 << 20;
  options.level0_file_num_compaction_trigger = 2;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_bytes_for_level_base = options.max_bytes_for_level_base * options.max_bytes_for_level_multiplier;
  auto table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));

  table_options->block_cache = rocksdb::NewLRUCache(5000, 0);
  // table_options->no_block_cache = true;

  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
}

rocksdb::Options get_levelcache_options(int cache_size = 0) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4 << 20;
  options.level0_file_num_compaction_trigger = 2;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_bytes_for_level_base = options.max_bytes_for_level_base * options.max_bytes_for_level_multiplier;
  auto table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));
  options.extern_options = new rocksdb::ExternOptions();
  table_options->no_block_cache = true;
  // auto block_cache = rocksdb::NewLRUCache(8000000, 0);
  // auto block_cache = rocksdb::NewHeapCache(113664000, 0);
  // table_options->block_cache = block_cache;
  // options.extern_options->block_cache = block_cache;

  options.extern_options->NewLevelCache(cache_size * 1024, 7);
  

  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rocksdb::DB* db;

  rocksdb::Options options;
  if (FLAGS_workload == "test" && FLAGS_cache_size > 0) {
    options = get_levelcache_options(FLAGS_cache_size);
  }
  else {
    options = get_default_options();
  }  
  auto read_options = rocksdb::ReadOptions();
  if (FLAGS_workload == "test") {
    options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
    options.disable_auto_compactions = true;
    if (options.extern_options)
    {
      read_options.extern_options = new rocksdb::ExternOptions();
      read_options.extern_options->level_cache = options.extern_options->level_cache;
    }
    read_options.verify_checksums = false;
  }
  // options.level0_slowdown_writes_trigger = 4;
  // options.level0_stop_writes_trigger = 8;
  options.statistics = rocksdb::CreateDBStatistics();
  options.advise_random_on_open = false;
  options.compression = rocksdb::kNoCompression;

  auto status = rocksdb::DB::Open(options, FLAGS_path, &db);
  if (!status.ok()) {
    std::cerr << "Failed to open db: " << status.ToString() << std::endl;
    return 1;
  }

  std::cout << "cache_capacity: " << FLAGS_cache_size << " entries" << std::endl;

  TestStats test_stats;

  if (FLAGS_workload == "prepare") {
    PrepareDB_ingestion(db, options);
    // FileWorkload(db, test_stats);
  } else if (FLAGS_workload == "test") {
    FileWorkload(db, test_stats, &read_options);
  }
  std::string stat;
  db->GetProperty("rocksdb.stats", &stat);
  std::cout << stat << std::endl;
  std::cout << "statistics: " << options.statistics->ToString() << std::endl;

  if (FLAGS_workload == "test")
  {
    std::ofstream out("/home/jiarui/CacheLSM/results", std::ios::app);
    out<<"bpk: "<<FLAGS_bpk<<std::endl;
    auto op_time = test_stats.OP_time / test_stats.OP_count;
    out<<"OP time: "<<op_time<<std::endl;
    auto hit_rate = (double)test_stats.n_hit/test_stats.OP_count;
    out<<"hit rate: "<<(double)test_stats.n_hit/test_stats.OP_count<<std::endl;
    out<<"get time: "<<(double)test_stats.get_time/test_stats.n_get<<std::endl;
    out<<"put time: "<<(double)test_stats.put_time/test_stats.n_put<<std::endl;
    out<<"scan time: "<<(double)test_stats.scan_time/test_stats.n_scan<<std::endl;
    double ratio_in_cache = (double)test_stats.length_in_cache / (test_stats.length_in_cache + test_stats.length_in_db);
    out<<"ratio in cache: "<< ratio_in_cache <<std::endl;
    out<<"scan time in cache: " << test_stats.time_in_cache / test_stats.length_in_cache * 16<<std::endl;
    out<<"scan time in db: " << test_stats.time_in_db / test_stats.length_in_db * 16<<std::endl;
    out<<"avg in db length: "<< (double)test_stats.length_in_db/test_stats.n_scan<<std::endl;

    out<<"filter_memory: "<<actual_filter_size<<std::endl;
    auto full_positive = options.statistics->getTickerCount(rocksdb::BLOOM_FILTER_FULL_POSITIVE);
    auto usefull = options.statistics->getTickerCount(rocksdb::BLOOM_FILTER_USEFUL);
    auto full_true_positive = options.statistics->getTickerCount(rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE);
    out<<"FPR: "<< (double)(full_positive - full_true_positive)/(full_positive - full_true_positive + usefull)<<std::endl;
    rocksdb::HistogramData hist;
    options.statistics->histogramData(rocksdb::DB_GET, &hist);
    auto db_get_time = hist.average;
    out<<"db get time: "<<db_get_time<<std::endl;
    options.statistics->histogramData(rocksdb::SST_READ_MICROS, &hist);
    out<<"sst read time: "<<hist.average<<std::endl;
    out<<"cache time: "<<op_time - (1-hit_rate)*db_get_time<<std::endl;
  }

  // for some reason, heap cache is not destructed properly
  // so we need to manually call the report function
  auto block_cache = options.extern_options ? options.extern_options->block_cache : nullptr;
  if (block_cache)
  {
    auto c = block_cache.get();
    string name = c->Name();
    if (name == "HeapCache")
      c->Lookup("report");  
  }

  db->Close();
  delete db;
  return 0;
}