#include <span>
#include <atomic>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "tools/data_structure.h"

using namespace std;
size_t counter = 0;
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
  // double get_hitrate(int n_levels, bool block_only = false) {
  //   if (block_only)
  //     return (double)(n_block_hit) / (1 + n_block_get);
  //   return (double)(n_block_hit + n_hit + length_in_cache / 4 + (n_scan -
  //   n_in_db) * (n_levels + 1)) /
  //     (1 + n_get + scan_length / 4 + n_scan * (n_levels + 1));
  // }
  double get_hitrate(int n_levels) {
    return 1 - 1.0 * n_block_miss /
                   (1 + n_get + scan_length / 4 + n_scan * (n_levels + 1));
  }
};

rocksdb::Status get_with_cache(rocksdb::DB* db, Cache* cache, std::string& key,
                               std::string& value, TestStats& stats) {
  if (cache && cache->get(key, value)) {
    stats.n_hit++;
    return rocksdb::Status::OK();
  }

  auto read_options = rocksdb::ReadOptions();
  read_options.extern_options = new rocksdb::ExternOptions();

  auto status = db->Get(read_options, key, &value);
  if (!status.ok()) return status;

  int hit_level = read_options.extern_options->hit_level;
  int n_IO = read_options.extern_options->n_IO;

  if (cache) {
    cache->put(key, value, hit_level);
  }
  delete read_options.extern_options;
  return status;
}

void range_cache_put(rocksdb::DB* db, RangeCache* cache, const std::string key,
                     const std::string value, TestStats& stats) {
#if OWN_SKIPLIST_IMPL
  auto iter = cache->hash.find(key);
  if (iter != cache->hash.end()) {
    auto node = iter->second;
    node->value = value;
    return;
  }
  // if in range and not in cache
  // auto r = cache->ranges.lower_bound(key);
  // if (r != cache->ranges.end() && r->second <= key)
  // {
  //   cache->insert(key, value);
  //   return;
  // }
#else
  auto node = cache->skip_list->find(key);
  if (node) {
    node->value = value;
    cache->last_node = nullptr;
    return;
  }
#endif
}

rocksdb::Status put_with_cache(rocksdb::DB* db, Cache* cache, std::string& key,
                               std::string& value, TestStats& stats) {
  auto status = db->Put(rocksdb::WriteOptions(), key, value);
  if (!status.ok()) return status;

  if (cache && cache->name() == "Range") {
    auto range_cache = dynamic_cast<RangeCache*>(cache);
    range_cache_put(db, range_cache, key, value, stats);
    return status;
  }
  else if (cache && cache->name() == "LeCaR") {
    auto lecar_cache = dynamic_cast<RangeCache_LeCaR*>(cache);
    auto node = lecar_cache->skip_list->search(key);
    if (node && node->key == key) {
      node->value = value;
    }
  }

  string v;
  if (cache && cache->get(key, v)) {
    cache->put(key, value);
  }
  return status;
}

rocksdb::Status delete_with_cache(rocksdb::DB* db, Cache* cache,
                                  std::string& key, TestStats& stats) {
  auto status = db->Delete(rocksdb::WriteOptions(), key);
  if (!status.ok()) return status;

  if (cache) {
    cache->remove(key);
  }
  return status;
}
#define RANGE_INSERT_TINYLFU 0
void range_cache_scan_with_len(rocksdb::DB* db, rocksdb::ReadOptions& ro, RangeCache* cache,
                               const std::string key, size_t length,
                               size_t limit, TestStats& stats) {
  auto start = chrono::steady_clock::now();
  string db_seek_key = key;
  string db_stop_key = "";
  bool cache_available = false;
  if (cache && cache->get_capacity()) {
    cache_available = true;
  }
  while (length)
  {  
    if (cache_available) {
      auto r = cache->ranges.lower_bound(key);
      if (r != cache->ranges.end() && r->second <= key) {
        stats.n_in_cache++;
  #if OWN_SKIPLIST_IMPL
        auto node = cache->skip_list->search(key);
        cache->access(node->data);
        while (length) {
          if (node->get_next() != cache->skip_list->tail && node->data <= r->first) {
            node = node->get_next();
            cache->access(node->data);
            stats.length_in_cache++;
            length--;
          } else {
            // node = node->get_prev();
            db_seek_key = node->data;
            break;
          }
        }
  #else
        auto node = cache->skip_list->search(key);
        if (node == cache->skip_list->head()) {
          throw std::runtime_error("node is head");
        }
        cache->skip_list->LRU_append(node);
        while (length) {
          if (node->next() && node->next()->key <= r->first) {
            node = node->next();
            cache->skip_list->LRU_append(node);
            stats.length_in_cache++;
            length--;
          } else {
            break;
          }
        }
        if (length) {
          db_seek_key = node->key.to_string();
          r++;
          db_stop_key = r->second.to_string();
        }
  #endif
        auto end = chrono::steady_clock::now();
        stats.time_in_cache +=
            chrono::duration_cast<chrono::microseconds>(end - start).count();
      }
    }
    if (length) {
      vector<pair<string, string>> range;
      start = chrono::steady_clock::now();
      auto iter = db->NewIterator(ro);
      string end_key = db_seek_key;
      iter->Seek(db_seek_key);
      if (!iter->Valid()) {
        delete iter;
        return;
      }
      stats.n_in_db++;
      string insert_key = iter->key().ToString();
      string insert_value = iter->value().ToString();
      if (cache_available)
  #if RANGE_INSERT_TINYLFU
        range.push_back({insert_key, insert_value});
  #else
        cache->insert(insert_key, insert_value);
  #endif
      while (length) {
        iter->Next();
        length--;
        stats.length_in_db++;
        if (iter->Valid()) {
          assert(iter->key().ToString().length() > 0);
          
          if (limit && cache_available) {
            insert_key = iter->key().ToString();
            insert_value = iter->value().ToString();
  #if RANGE_INSERT_TINYLFU
            range.push_back({insert_key, insert_value});
  #else
            cache->insert(insert_key, insert_value, true);
  #endif
            limit--;
          }
          if (false && cache_available && db_stop_key != "" && iter->key().ToString() >= db_stop_key) {
            db_seek_key = iter->key().ToString();
            db_stop_key = "";
            break;
          }
        } else {
          length = 0;
          break;
        }
      }
      delete iter;
      if (cache_available)
  #if RANGE_INSERT_TINYLFU
        cache->insert_range(range);
  #else
        cache->update_range(db_seek_key, insert_key);
  #endif
      auto end = chrono::steady_clock::now();
      stats.time_in_db +=
          chrono::duration_cast<chrono::microseconds>(end - start).count();
    }
  }
}

void LeCaR_scan(rocksdb::DB* db, RangeCache_LeCaR* cache, const std::string key,
                size_t length, TestStats& stats) {
  auto start = chrono::steady_clock::now();
  string db_seek_key = key;
  auto r = cache->ranges.lower_bound(key);
  if (r != cache->ranges.end() && r->second <= key) {
    stats.n_in_cache++;
    auto node = cache->skip_list->search(key);
    cache->access(node);

    while (length) {
      node->get_next();
      if (node != cache->skip_list->tail && node->key <= r->first) {
        cache->access(node);
        stats.length_in_cache++;
        length--;
      } else {
        node = node->get_prev();
        db_seek_key = node->key;
      }
    }
    auto end = chrono::steady_clock::now();
    stats.time_in_cache +=
        chrono::duration_cast<chrono::microseconds>(end - start).count();
  }

  if (length) {
    start = chrono::steady_clock::now();
    auto iter = db->NewIterator(rocksdb::ReadOptions());
    string end_key = db_seek_key;
    iter->Seek(db_seek_key);
    assert(iter->Valid());
    stats.n_in_db++;
    vector<pair<string, string>> range;
    range.push_back({iter->key().ToString(), iter->value().ToString()});
    while (length) {
      iter->Next();
      length--;
      stats.length_in_db++;
      if (iter->Valid()) {
        assert(iter->key().ToString().length() > 0);
        range.push_back({iter->key().ToString(), iter->value().ToString()});
        end_key = iter->key().ToString();
      } else
        break;
    }
    delete iter;
    auto end = chrono::steady_clock::now();
    stats.time_in_db +=
        chrono::duration_cast<chrono::microseconds>(end - start).count();
    cache->insert_range(range);
  }
}

rocksdb::Status scan_with_cache(rocksdb::DB* db, Cache* cache,
                                const std::string& key, size_t length,
                                TestStats& stats) {
  bool use_cache = true;
  if (cache && cache->name() == "Range") {
    auto range_cache = dynamic_cast<RangeCache*>(cache);
    auto read_options = rocksdb::ReadOptions();
    range_cache_scan_with_len(db, read_options, range_cache, key, length, length, stats);
  } else if (cache && cache->name() == "LeCaR") {
    auto lecar_cache = dynamic_cast<RangeCache_LeCaR*>(cache);
    LeCaR_scan(db, lecar_cache, key, length, stats);
  } else {
    auto iter = db->NewIterator(rocksdb::ReadOptions());
    iter->Seek(key);
    for (size_t i = 0; i < length && iter->Valid(); i++) {
      iter->Next();
    }
    delete iter;
  }
  return rocksdb::Status::OK();
}
