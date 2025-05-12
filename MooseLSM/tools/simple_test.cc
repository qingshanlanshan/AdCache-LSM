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
#include "util/string_util.h"

using std::string;

DEFINE_string(level_capacities, "4194304,41943040,419430400,4194304000",
              "41943040000"
              "Comma-separated list of level capacities");
DEFINE_string(run_numbers, "1,1,1,1,1", "Comma-separated list of run numbers");
DEFINE_string(compaction_style, "moose", "Compaction style");
DEFINE_int32(bpk, 10, "Bits per key for filter");
DEFINE_int32(kvsize, 1024, "Size of key-value pair");

class Timer {
 public:
  Timer() : start_(std::chrono::system_clock::now()) {}
  ~Timer() {
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now() - start_);
    std::cout << microseconds.count() << "us" << std::endl;
  }

 private:
  std::chrono::time_point<std::chrono::system_clock> start_;
};

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

  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));

  table_options->no_block_cache = true;
  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
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
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->filter_policy.reset(rocksdb::NewBloomFilterPolicy(FLAGS_bpk));

  // auto block_cache = rocksdb::NewLRUCache(2<<30);
  table_options->no_block_cache = true;


  table_options->block_size = 4096;
  table_options->max_auto_readahead_size = 0;
  return options;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rocksdb::DB* db;

  rocksdb::Options options;

  options = get_moose_options();
  // options = get_default_options();

  // options.use_direct_io_for_flush_and_compaction = true;
  // options.use_direct_reads = true;

  options.statistics = rocksdb::CreateDBStatistics();
  options.advise_random_on_open = false;
  options.compression = rocksdb::kNoCompression;
  auto table_options =
      options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  table_options->max_auto_readahead_size = 0;
  // table_options->no_block_cache = true;
  auto status = rocksdb::DB::Open(options, "/home/jiarui/CacheLSM/db_20g", &db);
  if (!status.ok()) {
    std::cerr << "Failed to open db: " << status.ToString() << std::endl;
    return 1;
  }

  string filename = "/home/jiarui/CacheLSM/workload/test_query.dat";
  string line;
  std::ifstream file(filename);
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string op;
    string key;
    iss >> op;
    iss >> key;

    string value;
    // db->Get(rocksdb::ReadOptions(), key, &value);
    status = db->Put(rocksdb::WriteOptions(), key, "value");
    if (!status.ok()) {
      std::cerr << "Failed to put: " << status.ToString() << std::endl;
      return 1;
    }
  }

  // rocksdb::Iterator* iter = nullptr;
  // {
  //   Timer t;
  //   iter = db->NewIterator(read_options);
  // }
  // for (int n = 0; n < 1; ++n) {
  //   {
  //     Timer t;
  //     // about 4 blocks
  //     iter->Seek("0000011418111400");
  //   }
  //   // {
  //   //   Timer t;
  //   //   std::string value;
  //   //   for (int i = 0; i < 16; i++) {
  //   //     iter->Next();
  //   //   }
  //   // }
  // }
  // delete iter;

  std::string stat;
  db->GetProperty("rocksdb.stats", &stat);
  std::cout << stat << std::endl;
  std::cout << "statistics: " << options.statistics->ToString() << std::endl;

  db->Close();
  delete db;

  return 0;
}