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
// #include "rocksdb/cache.h"
#include "rocksdb/advanced_cache.h"

#include "rocksdb/options.h"
#include "tools/data_structure.h"

using std::string;

DEFINE_string(level_capacities, "4194304,41943040,419430400", "Comma-separated list of level capacities");
DEFINE_string(run_numbers, "1,1,1", "Comma-separated list of run numbers");
DEFINE_int32(bpk, 5, "Bits per key for filter");
DEFINE_int32(kvsize, 1024, "Size of key-value pair");
DEFINE_string(compaction_style, "moose", "Compaction style");
DEFINE_int32(prepare_entries, 10000000, "Number of entries to prepare");
DEFINE_string(workload, "prepare", "prepare or test");
DEFINE_string(path, "/tmp/db", "dbpath");
DEFINE_int32(N, 64000, "Number of entries");
DEFINE_int32(memery_budget, 0, "Memory budget");
DEFINE_string(workload_file, "workload", "workload file");
DEFINE_int32(cache_size, 0, "Cache size");
DEFINE_int32(sst_size, 4194304, "SST size");
DEFINE_string(cache_style, "level", "Cache style: level, LRU, ELRU, SetAssociative, heap");
DEFINE_int32(use_cmsketch, 0, "Use CountMinSketch or not");

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

void PrepareDB(rocksdb::DB* db) {
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;
  KeyGenerator key_gen(0, FLAGS_prepare_entries, 24, FLAGS_kvsize - 24);
  key_gen.SeekToFirst();
  int idx = 0;
  while (key_gen.Next()) {
    auto status = db->Put(write_options, key_gen.Key(), key_gen.Value());
    if (!status.ok()) {
      std::cerr << "Failed to put key " << key_gen.Key() << " value "
                << key_gen.Value() << ", because: " << status.ToString() << std::endl;
      exit(1);
    }
    idx ++;
    if (idx % 100000 == 0) {
      std::cout << "prepared: " << idx << " entries" << std::endl;
    }
  }
}

struct TestStats {
  double OP_time = 0;
  size_t OP_count = 0;
  size_t hit = 0;
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
  KeyGenerator key_gen(0, FLAGS_prepare_entries, 24, FLAGS_kvsize - 24);
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
    // if (operation.key.size() < 24) {
    //   // fill the key with 0 in the front
    //   operation.key = ItoaWithPadding(std::stoull(operation.key), 24);
    // }
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
    table_options->filter_policy.reset(rocksdb::NewMonkeyFilterPolicy(monkey_bpks));
    rocksdb::SstFileWriter writer(rocksdb::EnvOptions(), options);
    writer.Open(file);
    for (auto& op : ops_cur_level) {
      writer.Put(op.key, op.value);
    }
    writer.Finish();
    auto status = db->IngestExternalFile({file}, rocksdb::IngestExternalFileOptions());
  }

}

rocksdb::Status get_with_cache(rocksdb::DB* db, Cache* cache, const std::string& key, std::string& value, TestStats& stats) {
  if (FLAGS_use_cmsketch)
    cmsketch.insert(key, 1);

  if (cache && cache->get(key, value)) {
    stats.hit ++;
    return rocksdb::Status::OK();
  }

  auto read_options = rocksdb::ReadOptions();
  read_options.extern_options = new rocksdb::ExternOptions();

  auto status = db->Get(read_options, key, &value);
  if (!status.ok())
    return status;

  int hit_level = read_options.extern_options->hit_level;
  int n_IO = read_options.extern_options->n_IO;

  if (FLAGS_use_cmsketch && cache)
  {
    cmsketch.insert(key, hit_level);
    if (cmsketch.get_sum_counter() > cache->get_capacity() && !cmsketch.query(key))
      return status;
  }
  
  if (cache)
  {
    cache->put(key, value, hit_level);
    // cache->put(key, value, n_IO);
  }
  return status;
}

void FileWorkload(rocksdb::DB* db, TestStats& stats, Cache* cache = nullptr)
{
  rocksdb::ReadOptions read_options;
  read_options.extern_options = new rocksdb::ExternOptions();
  read_options.verify_checksums = false;

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
      std::string value;
      // auto status = db->Get(read_options, op.key, &value);
      auto status = get_with_cache(db, cache, op.key, value, stats);
      if (!status.ok())
      {
        std::cerr << "Failed to get key " << op.key << std::endl;
        exit(1);
      }
    }
    else if (op.type == PUT)
    {
      auto status = db->Put(rocksdb::WriteOptions(), op.key, op.value);
      if (!status.ok())
      {
        std::cerr << "Failed to put key " << op.key << " value " << op.value << std::endl;
        exit(1);
      }
    }
    else if (op.type == DELETE)
    {
      auto status = db->Delete(rocksdb::WriteOptions(), op.key);
      if (!status.ok())
      {
        std::cerr << "Failed to delete key " << op.key << std::endl;
        exit(1);
      }
    }
    else if (op.type == SCAN)
    {
      rocksdb::Iterator* it = db->NewIterator(read_options);
      it->Seek(op.key);
      if (!it->Valid())
      {
        std::cerr << "Failed to seek to key " << op.key << std::endl;
        exit(1);
      }
      for (int i = 0; i < 16 && it->Valid(); i++)
      {
        it->Next();
      }
      delete it;
    }
  }
  auto end = std::chrono::steady_clock::now();
  stats.OP_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  stats.OP_count = operations.size();
}

void BalancedWorkload(rocksdb::DB* db) {
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;
  KeyGenerator key_gen(0, FLAGS_prepare_entries, 24, FLAGS_kvsize - 24);
  key_gen.SeekToFirst();
  int idx = 0;
  while (key_gen.Next()) {
    int mo = idx % 4;
    switch (mo) {
      case 0: {
        // get result
        std::string value;
        auto status = db->Get(read_options, key_gen.Key(), &value);
        if (!status.ok()) {
          std::cerr << "Failed to get key " << key_gen.Key() << std::endl;
          exit(1);
        }
        break;
      }
      case 1: {
        // empty result
        auto key = key_gen.Key();
        std::string value;
        key[key.size() - 2] = '_';
        auto status = db->Get(read_options, key, &value);
        if (!status.IsNotFound()) {
          std::cerr << "Found key " << key << " value " << value << std::endl;
          exit(1);
        }
        break;
      }
      case 2: {
        // put
        auto status = db->Put(write_options, key_gen.Key(), key_gen.Value());
        if (!status.ok()) {
          std::cerr << "Failed to put key " << key_gen.Key()
                    << " value " << key_gen.Value() << std::endl;
          exit(1);
        }
        break;
      }
      case 3: {
        // range read
        rocksdb::Iterator* it = db->NewIterator(read_options);
        auto key = key_gen.Key();
        it->Seek(key);
        if (!it->Valid()) {
          std::cerr << "Failed to seek to key " << key << std::endl;
          exit(1);
        }
        for (int i = 0; i < 16 && it->Valid(); i++) {
          it->Next();
        }
        delete it;
        break;
      }
    }
    idx ++;
    if (idx % 100000 == 0) {
      std::cout << "conducted " << idx << " operations" << std::endl;
    }
  }
}

rocksdb::Options get_default_options() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 2 << 20;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_bytes_for_level_base = options.max_bytes_for_level_base * options.max_bytes_for_level_multiplier;
  auto table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));
  
  auto block_cache = rocksdb::NewLRUCache(32000000, 0);
  // auto block_cache = rocksdb::NewHeapCache(32000000, 0);
  table_options->block_cache = block_cache;
  options.extern_options = new rocksdb::ExternOptions();
  options.extern_options->block_cache = block_cache;

  // table_options->no_block_cache = true;

  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
}

rocksdb::Options get_moose_options() {
  rocksdb::Options options;
  options.compaction_style = rocksdb::kCompactionStyleMoose;
  options.create_if_missing = true;
  options.write_buffer_size = 2 << 20;
  options.level_compaction_dynamic_level_bytes = false;
  
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
  options.num_levels = std::accumulate(run_numbers.begin(), run_numbers.end(), 0);

  uint64_t entry_num = std::accumulate(options.level_capacities.begin(), options.level_capacities.end(), 0UL) / FLAGS_kvsize;
  uint64_t filter_memory = entry_num * FLAGS_bpk / 8;

  auto tmp = rocksdb::MonkeyBpks(entry_num, filter_memory, options.level_capacities, FLAGS_kvsize);
  std::vector<double> bpks = {};
  std::copy(tmp.begin(), tmp.end(), std::back_inserter(bpks));
  // display options
  std::cout << "level capacities: " << std::endl;
  for (auto lvl_cap : options.level_capacities) {
    std::cout << "  " << lvl_cap << std::endl;
  }
  std::cout << "run numbers: " << std::endl;
  for (auto rn : options.run_numbers) {
    std::cout << "  " << rn << std::endl;
  }
  std::cout << "bpks: " << std::endl;
  for (auto bpk : bpks) {
    std::cout << "  " << bpk << std::endl;
  }
  monkey_bpks = bpks;

  if (FLAGS_workload == "test")
  {
    int N = FLAGS_N;
    std::cout << "N: " << N << std::endl;
    for (size_t i = 0; i < bpks.size(); i++) {
      N -= options.level_capacities[i] / FLAGS_kvsize;
      if (N < 0)
        break;
      std::cout << "filter size at level " << i << " is " << bpks[i] / 8 * options.level_capacities[i] / FLAGS_kvsize << std::endl;
      actual_filter_size += bpks[i] / 8 * options.level_capacities[i] / FLAGS_kvsize;
    }
    std::cout << "actual filter size: " << actual_filter_size << std::endl;
  }
  auto table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewMonkeyFilterPolicy(bpks));
  
  // auto block_cache = rocksdb::NewLRUCache(32000000, 0);
  auto block_cache = rocksdb::NewHeapCache(32000000, 0);
  table_options->block_cache = block_cache;
  options.extern_options = new rocksdb::ExternOptions();
  options.extern_options->block_cache = block_cache;

  // table_options->no_block_cache = true;

  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rocksdb::DB* db;

  rocksdb::Options options;
  if (FLAGS_compaction_style == "default") {
    options = get_default_options();
  } else if (FLAGS_compaction_style == "moose") {
    options = get_moose_options();
  } else {
    std::cerr << "Unknown compaction style: " << FLAGS_compaction_style << std::endl;
    return 1;
  }
  if (FLAGS_workload == "test") {
    options.use_direct_io_for_flush_and_compaction = true;
    options.use_direct_reads = true;
    // options.disable_auto_compactions = true;
  }
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 8;
  options.statistics = rocksdb::CreateDBStatistics();
  options.advise_random_on_open = false;
  options.compression = rocksdb::kNoCompression;

  auto status = rocksdb::DB::Open(options, FLAGS_path, &db);
  if (!status.ok()) {
    std::cerr << "Failed to open db: " << status.ToString() << std::endl;
    return 1;
  }


  Cache* cache = nullptr;
  int cache_capacity = 0;

  if (FLAGS_workload == "test") {
    if (FLAGS_memery_budget > 0) {
      if (FLAGS_memery_budget > (int)actual_filter_size + FLAGS_kvsize)
        cache_capacity = (FLAGS_memery_budget - actual_filter_size) / FLAGS_kvsize;
      else
        exit(1); 
    }
    else if (FLAGS_cache_size > 0)
    {
      cache_capacity = FLAGS_cache_size;
    }
  }
  std::cout<<"cache_capacity: "<<cache_capacity<<std::endl;
  if (cache_capacity > 0)
  {
    if (FLAGS_cache_style == "level")
    {
      cache = new LevelCache(cache_capacity, options.level_capacities.size());
    }
    else if (FLAGS_cache_style == "LRU")
    {
      cache = new LRUCache(cache_capacity);
    }
    else if (FLAGS_cache_style == "ELRU")
    {
      cache = new ELRU(cache_capacity, 20);
    }
    else if (FLAGS_cache_style == "SetAssociative")
    {
      cache = new SetAssociativeCache(cache_capacity, 16);
    }
    else if (FLAGS_cache_style == "heap")
    {
      cache = new HeapCache(cache_capacity);
    }
  }

  TestStats test_stats;

  if (FLAGS_workload == "prepare") {
    // PrepareDB(db);
    FileWorkload(db, test_stats);
    // PrepareDB_ingestion(db, options);
  } else if (FLAGS_workload == "test") {
    // BalancedWorkload(db);
    FileWorkload(db, test_stats, cache);
  }
  std::string stat;
  db->GetProperty("rocksdb.stats", &stat);
  std::cout << stat << std::endl;
  std::cout << "statistics: " << options.statistics->ToString() << std::endl;

  if (FLAGS_workload == "test")
  {
    std::ofstream out("/home/jiarui/CacheLSM/results", std::ios::app);
    out<<"bpk: "<<FLAGS_bpk<<std::endl;
    if (cache)
      out<<"cache_size: "<<cache_capacity<<std::endl;
    else
      out<<"cache_size: 0"<<std::endl;
    auto op_time = test_stats.OP_time / test_stats.OP_count;
    out<<"OP time: "<<op_time<<std::endl;
    auto hit_rate = (double)test_stats.hit/test_stats.OP_count;
    out<<"hit rate: "<<(double)test_stats.hit/test_stats.OP_count<<std::endl;
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
  auto block_cache = options.extern_options->block_cache;
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