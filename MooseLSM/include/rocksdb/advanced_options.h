// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <map>
#include <algorithm>
#include <iostream>
#include <cmath>
#include <chrono>

#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/universal_compaction.h"

using std::string;

namespace ROCKSDB_NAMESPACE {

class Slice;
class SliceTransform;
class TablePropertiesCollectorFactory;
class TableFactory;
struct Options;

class RangeCache {
public:
  struct SkipListNode {
    std::vector<SkipListNode*> next;
    std::vector<SkipListNode*> prev;
    SkipListNode* lru_prev;
    SkipListNode* lru_next;
    uint64_t timestamp;
    string key;
    string value;
    SkipListNode* get_next(int i = 0) { return next[i]; }
    SkipListNode* get_prev(int i = 0) { return prev[i]; }
  };
  struct SkipList {
    SkipList() {
      head = new SkipListNode();
      tail = new SkipListNode();
      head->next.push_back(tail);
      tail->prev.push_back(head);
      level = 1;
      lru_head = new SkipListNode();
      lru_tail = new SkipListNode();
      lru_head->lru_next = lru_tail;
      lru_tail->lru_prev = lru_head;
      size = 0;
    }
    ~SkipList() {
      SkipListNode* node = head->get_next();
      while (node != tail) {
        SkipListNode* next = node->get_next();
        delete node;
        node = next;
      }
      delete head;
      delete tail;
      delete lru_head;
      delete lru_tail;
    }
    SkipListNode* search(string key) {
      SkipListNode* node = head;
      assert(level == node->next.size());
      for (int i = level - 1; i >= 0; i--) {
        while (node->next[i] != tail && node->next[i]->key <= key) {
          node = node->next[i];
        }
      }
      if (node == head) return nullptr;
      return node;
    }
    void insert(std::string key, std::string value) {
      SkipListNode* node = head;
      std::vector<SkipListNode*> update(level, nullptr);
      for (int i = level - 1; i >= 0; i--) {
        while (node->next[i] != tail && node->next[i]->key < key) {
          node = node->next[i];
        }
        update[i] = node;
      }
      if (node->next[0] && node->next[0]->key == key) {
        node->next[0]->value = value;
        return;
      }
      SkipListNode* new_node = new SkipListNode();
      new_node->key = key;
      new_node->value = value;
      size_t new_level = 1;
      while (rand() % 2 == 0) {
        new_level++;
      }
      if (new_level > level) {
        for (size_t i = level; i < new_level; i++) {
          update.push_back(head);
          head->next.push_back(tail);
          tail->prev.push_back(head);
        }
        level = new_level;
      }
      for (size_t i = 0; i < new_level; i++) {
        new_node->next.push_back(update[i]->next[i]);
        new_node->prev.push_back(update[i]);
        update[i]->next[i]->prev[i] = new_node;
        update[i]->next[i] = new_node;
      }

      // lru
      lru_append(new_node);
      size++;
    }
    void remove(std::string key) {
      SkipListNode* node = head;
      std::vector<SkipListNode*> update(level, nullptr);
      for (int i = level - 1; i >= 0; i--) {
        while (node->next[i] != tail && node->next[i]->key < key) {
          node = node->next[i];
        }
        update[i] = node;
      }
      if (node->next[0] && node->next[0]->key == key) {
        SkipListNode* target = node->next[0];
        // lru
        lru_remove(target);
        // skip list
        for (size_t i = 0; i < level; i++) {
          if (update[i]->next[i] == target) {
            update[i]->next[i] = target->next[i];
            target->next[i]->prev[i] = update[i];
          }
        }
        delete target;
        size--;
      }
    }

    void lru_remove(SkipListNode* node) {
      node->lru_prev->lru_next = node->lru_next;
      node->lru_next->lru_prev = node->lru_prev;
    }
    void lru_append(SkipListNode* node) {
      node->lru_prev = lru_tail->lru_prev;
      node->lru_next = lru_tail;
      lru_tail->lru_prev->lru_next = node;
      lru_tail->lru_prev = node;
      node->timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
                             .count();
    }

    void print() {
      for (int i = level - 1; i >= 0; i--) {
        SkipListNode* node = head;
        std::cout << "level " << i << ": " << "h" << "->";
        while (node->next[i] != tail) {
          std::cout << node->next[i]->key << "->";
          node = node->next[i];
        }
        std::cout << "e" << std::endl;
      }
    }
    SkipListNode* head;
    SkipListNode* tail;
    size_t level;
    // old
    SkipListNode* lru_head;
    // new
    SkipListNode* lru_tail;
    size_t size;
  };

 public:
  RangeCache() {
    skip_list = new SkipList();
  }
  ~RangeCache() { delete skip_list; }
  size_t get_size() { return skip_list->size; }
  void insert(string key, string value) {
    skip_list->insert(key, value);
  }

  void insert_range(std::vector<std::pair<string, string>>& range, bool merge_range_with_prev = false) {
    string start = range[0].first;
    string end = range[range.size() - 1].first;
    
    // merge with ranges
    auto item = ranges.lower_bound(start);
    if (merge_range_with_prev && item != ranges.begin())
    {
      item--;
      // likely
      if (item != ranges.begin())
      {
        string new_start = item->second;
        string new_end = end;
        ranges.erase(item);
        ranges[new_end] = new_start;
      }
    }
    // if overlap
    else if (item != ranges.end() && end >= item->second) {
      string old_start = item->second;
      string old_end = item->first;
      string new_start = min(start, old_start);
      string new_end = max(end, old_end);
      if (new_end == old_end) {
        item->second = new_start;
      } else {
        ranges.erase(item);
        ranges[new_end] = new_start;
      }
    } else {
      ranges[end] = start;
    }

    // for (auto& r : range) {
    //   insert(r.first, r.second);
    // }
    for (auto iter = range.rbegin(); iter != range.rend(); iter++) {
      insert(iter->first, iter->second);
    }
  }

  void remove(string key) {
    auto node = skip_list->search(key);
    assert(node && node->key == key);

    skip_list->lru_remove(node);
    // if key is in a range, it breaks the range into two
    auto item = ranges.lower_bound(key);
    if (item != ranges.end() && item->second <= key) {
      string start = item->second;
      string end = item->first;

      if (start == end) {
        ranges.erase(item);
      } else if (start == key) {
        item->second = node->get_next()->key;
      } else if (end == key) {
        ranges.erase(item);
        ranges[node->get_prev()->key] = end;
      } else {
        ranges[key] = node->get_next()->key;
        ranges[node->get_prev()->key] = key;
      }
    }
    skip_list->remove(key);
  }

  void evict() {
    auto node = skip_list->lru_head->lru_next;
    if (node == skip_list->lru_tail) {
      return;
    }
    remove(node->key);
  }

  void print() {
    skip_list->print();
    // print ranges
    for (auto& r : ranges) {
      std::cout << r.second << " -> " << r.first << std::endl;
    }
  }
  SkipListNode* search(string key) { 
    return skip_list->search(key); 
  }

  void merge(RangeCache& other) {
    std::vector<std::pair<string, string>> new_ranges;
    auto iter1 = ranges.begin();
    auto iter2 = other.ranges.begin();
    while (iter1 != ranges.end() && iter2 != other.ranges.end()) {
      std::pair<string, string> r1 = {iter1->second, iter1->first};
      std::pair<string, string> r2 = {iter2->second, iter2->first};

      // if no overlapping, go to next
      if (r1.second < r2.first) {
        new_ranges.push_back(r1);
        iter1++;
      } else if (r2.second < r1.first) {
        new_ranges.push_back(r2);
        iter2++;
      } else {
        // put the overlapping part
        string start = max(r1.first, r2.first);
        string end = min(r1.second, r2.second);
        new_ranges.push_back({start, end});
        if (r1.second < r2.second) {
          iter1++;
        } else {
          iter2++;
        }
      }
    }
    ranges.clear();
    for (auto& r : new_ranges) {
      ranges[r.second] = r.first;
    }

    auto node = other.skip_list->head->get_next();
    while (node != other.skip_list->tail) {
      skip_list->insert(node->key, node->value);
      node = node->get_next();
    }
  }

  void merge(RangeCache* other, string* from, string* to) {
    // from is inclusive, to is exclusive
    // first, merge ranges
    std::vector<std::pair<string, string>> new_ranges;
    auto from_range = other->ranges.begin();
    auto to_range = other->ranges.end();
    if (from) {
      from_range = other->ranges.lower_bound(*from);
    }
    if (to) {
      to_range = other->ranges.lower_bound(*to);
    }
    auto iter1 = ranges.begin();
    auto iter2 = from_range;
    while (iter1 != ranges.end() && iter2 != to_range) {
      std::pair<string, string> r1 = {iter1->second, iter1->first};
      std::pair<string, string> r2 = {iter2->second, iter2->first};
      bool put_back = false;
      std::pair<string, string> put_back_range;
      if (iter2 == from_range && *from > r2.first)
      {
        assert(r2.second >= *from);
        r2.first = *from;
        
        auto node = other->search(*from);
        if (node) {
          if (node->key == *from) {
            node = node->get_prev();
          }
          if (node == other->skip_list->head)
            node = nullptr;
        }
        if (node && node->key > r2.first)
        {
          put_back = true;
          put_back_range = {r2.first, *from};
        }
      }
      else if (iter2 == to_range && *to < r2.second)
      {
        assert(r2.first <= *to);
        r2.second = *to;
        put_back = true;
        put_back_range = {*to, r2.second};
      }

      if (r1.second < r2.first) {
        new_ranges.push_back(r1);
        iter1++;
      } else if (r2.second < r1.first) {
        new_ranges.push_back(r2);
        iter2++;
      } else {
        // put the overlapping part
        string start = max(r1.first, r2.first);
        string end = min(r1.second, r2.second);
        new_ranges.push_back({start, end});
        if (r1.second < r2.second) {
          iter1++;
        } else {
          other->ranges.erase(iter2++);
          if (put_back)
          {
            other->ranges[put_back_range.second] = put_back_range.first;
            put_back = false;
          }
        }
      }
    }
    ranges.clear();
    for (auto& r : new_ranges) {
      ranges[r.second] = r.first;
    }

    // second, merge skip list
    {
      auto node = other->skip_list->head->get_next();
      auto end_node = other->skip_list->tail;
      if (from) {
        node = other->search(*from);
      }
      if (to) {
        end_node = other->search(*to);
      }
      while (node != end_node) {
        skip_list->insert(node->key, node->value);
        node = node->get_next();
      }
    }

    // third, remove the merged items
    auto from_node = other->skip_list->head->get_next();
    if (from) {
      from_node = other->search(*from);
    }
    auto to_node = other->skip_list->tail;
    if (to) {
      to_node = other->search(*to);
      if (!to_node)
      {
        if (from_node)
          std::cout << "shouldn't happen" << std::endl;
        return;
      }
      if (to_node->key == *to) {
        to_node = to_node->get_next();
      }
    }
    
    for (auto node = from_node; node != to_node; node = node->get_next()) {
      other->skip_list->remove(node->key);
    }
  }

  bool get(string key, string& value) {
    auto node = skip_list->search(key);
    if (!node) {
      return false;
    }
    if (node->key != key) {
      return false;
    }
    skip_list->lru_remove(node);
    skip_list->lru_append(node);
    value = node->value;
    return true;
  }

  void access(SkipListNode* node) {
    if (node == skip_list->head || node == skip_list->tail) {
      return;
    }
    skip_list->lru_remove(node);
    skip_list->lru_append(node);
  }

  string name() { return "Range"; }

public:
  SkipList* skip_list;
  // range_end -> range_start
  std::map<string, string> ranges;
public:
class Iterator {
public:
  Iterator(RangeCache* cache) : cache_(cache) {
    node_ = cache_->skip_list->lru_head->lru_next;
  }
  bool Valid() {
    return node_ && node_ != cache_->skip_list->head && node_ != cache_->skip_list->tail && in_range(); 
  }
  bool in_range() {
    if (node_ && node_->key >= seek_range.first && node_->key <= seek_range.second) {
      return true;
    }
    return false;
  }
  Slice key() { return Slice(node_->key); }
  Slice value() { return Slice(node_->value); }
  Slice user_key() { return Slice(node_->key.c_str(), node_->key.size() - 8); }
  void Next() { node_ = node_->get_next(); cache_->access(node_); }
  void SeekToFirst() { node_ = cache_->skip_list->head->get_next(); }
  void Seek(std::string key)
  {
    auto range = cache_->ranges.lower_bound(key);
    if (range == cache_->ranges.end() || range->second > key) {
      node_ = nullptr;
      return;
    }
    seek_range = {range->second, range->first};
    node_ = cache_->search(range->second);
    if (node_ == nullptr) {
      return;
    }
    cache_->access(node_);
  }

// private:
  RangeCache* cache_;
  RangeCache::SkipListNode* node_;
  // update when seek, start key -> end key
  std::pair<string, string> seek_range;
};
};

class LevelCache {
private:
  std::vector<RangeCache*> rangecaches;
  size_t capacity;
public:
  LevelCache(size_t cap, int n_level) {
    capacity = cap;
    for (int i = 0; i < n_level; i++) {
      rangecaches.push_back(new RangeCache());
    }
  }
  ~LevelCache() {
    for (auto& rc : rangecaches) {
      delete rc;
    }
  }
  void reset(size_t cap, int n_level) {
    capacity = cap;
    for (auto& rc : rangecaches) {
      delete rc;
    }
    rangecaches.clear();
    for (int i = 0; i < n_level; i++) {
      rangecaches.push_back(new RangeCache());
    }
  }
  size_t get_size() {
    size_t size = 0;
    for (auto& rc : rangecaches) {
      size += rc->get_size();
    }
    return size * 1024;
  }
  void insert(int level, string key, string value) {
    rangecaches[level]->insert(key, value);
    evict();
  }
  void insert_range(int level, std::vector<std::pair<string, string>>& range, bool merge_range_with_prev = false) {
    rangecaches[level]->insert_range(range, merge_range_with_prev);
    evict();
  }
  void remove(string key, int level) {
    if (level >= 0)
      rangecaches[level]->remove(key);
    else {
      for (auto& rc : rangecaches) {
        rc->remove(key);
      }
    }
  }
  void evict() {
    while (get_size() > capacity) {
      size_t victim_level = 0;
      uint64_t oldest_time = UINT64_MAX;
      for (size_t i = 0; i < rangecaches.size(); i++) {
        auto item = rangecaches[i]->skip_list->lru_head->lru_next;
        if (item == rangecaches[i]->skip_list->lru_tail) {
          continue;
        }
        auto item_timestamp = item->timestamp;
        if (item_timestamp <= oldest_time) {
          oldest_time = item_timestamp;
          victim_level = i;
        }
      }
      rangecaches[victim_level]->evict();
    }
  }
  bool Get(string key, string& value, int level) {
    return rangecaches[level]->get(key, value);
  }
  bool Get(string key, string& value) {
    for (auto& rc : rangecaches) {
      if (rc->get(key, value)) {
        return true;
      }
    }
    return false;
  }
  RangeCache* get_rangecache(size_t level) {
    if (level >= rangecaches.size()) {
      return nullptr;
    }
    return rangecaches[level];
  }
  void merge(int from_level, int to_level, string* from, string* to) {
    assert(from_level <= to_level);
    assert(from_level >= 0 && (size_t)to_level < rangecaches.size());
    rangecaches[to_level]->merge(rangecaches[from_level], from, to);
  }
};

struct ExternOptions {
  int hit_level = 0;
  int n_IO = 0;
  std::shared_ptr<rocksdb::Cache> block_cache = nullptr;
  LevelCache* level_cache = nullptr;
  string cache_style;
  void NewLevelCache(size_t cap, int n_level) {
    level_cache = new LevelCache(cap, n_level);
  }
};

enum CompactionStyle : char {
  // level based compaction style
  kCompactionStyleLevel = 0x0,
  // Universal compaction style
  kCompactionStyleUniversal = 0x1,
  // FIFO compaction style
  kCompactionStyleFIFO = 0x2,
  // Disable background compaction. Compaction jobs are submitted
  // via CompactFiles().
  kCompactionStyleNone = 0x3,

  kCompactionStyleMoose = 0x4,
};

// In Level-based compaction, it Determines which file from a level to be
// picked to merge to the next level. We suggest people try
// kMinOverlappingRatio first when you tune your database.
enum CompactionPri : char {
  // Slightly prioritize larger files by size compensated by #deletes
  kByCompensatedSize = 0x0,
  // First compact files whose data's latest update time is oldest.
  // Try this if you only update some hot keys in small ranges.
  kOldestLargestSeqFirst = 0x1,
  // First compact files whose range hasn't been compacted to the next level
  // for the longest. If your updates are random across the key space,
  // write amplification is slightly better with this option.
  kOldestSmallestSeqFirst = 0x2,
  // First compact files whose ratio between overlapping size in next level
  // and its size is the smallest. It in many cases can optimize write
  // amplification.
  kMinOverlappingRatio = 0x3,
  // Keeps a cursor(s) of the successor of the file (key range) was/were
  // compacted before, and always picks the next files (key range) in that
  // level. The file picking process will cycle through all the files in a
  // round-robin manner.
  kRoundRobin = 0x4,
};

// Compression options for different compression algorithms like Zlib
struct CompressionOptions {
  // ==> BEGIN options that can be set by deprecated configuration syntax, <==
  // ==> e.g. compression_opts=5:6:7:8:9:10:true:11:false                  <==
  // ==> Please use compression_opts={level=6;strategy=7;} form instead.   <==

  // RocksDB's generic default compression level. Internally it'll be translated
  // to the default compression level specific to the library being used (see
  // comment above `ColumnFamilyOptions::compression`).
  //
  // The default value is the max 16-bit int as it'll be written out in OPTIONS
  // file, which should be portable.
  static constexpr int kDefaultCompressionLevel = 32767;

  // zlib only: windowBits parameter. See https://www.zlib.net/manual.html
  int window_bits = -14;

  // Compression "level" applicable to zstd, zlib, LZ4, and LZ4HC. Except for
  // kDefaultCompressionLevel (see above), the meaning of each value depends
  // on the compression algorithm. Decreasing across non-
  // `kDefaultCompressionLevel` values will either favor speed over
  // compression ratio or have no effect.
  //
  // In LZ4 specifically, the absolute value of a negative `level` internally
  // configures the `acceleration` parameter. For example, set `level=-10` for
  // `acceleration=10`. This negation is necessary to ensure decreasing `level`
  // values favor speed over compression ratio.
  int level = kDefaultCompressionLevel;

  // zlib only: strategy parameter. See https://www.zlib.net/manual.html
  int strategy = 0;

  // Maximum size of dictionaries used to prime the compression library.
  // Enabling dictionary can improve compression ratios when there are
  // repetitions across data blocks.
  //
  // The dictionary is created by sampling the SST file data. If
  // `zstd_max_train_bytes` is nonzero, the samples are passed through zstd's
  // dictionary generator (see comments for option `use_zstd_dict_trainer` for
  // detail on dictionary generator). If `zstd_max_train_bytes` is zero, the
  // random samples are used directly as the dictionary.
  //
  // When compression dictionary is disabled, we compress and write each block
  // before buffering data for the next one. When compression dictionary is
  // enabled, we buffer SST file data in-memory so we can sample it, as data
  // can only be compressed and written after the dictionary has been finalized.
  //
  // The amount of data buffered can be limited by `max_dict_buffer_bytes`. This
  // buffered memory is charged to the block cache when there is a block cache.
  // If block cache insertion fails with `Status::MemoryLimit` (i.e., it is
  // full), we finalize the dictionary with whatever data we have and then stop
  // buffering.
  uint32_t max_dict_bytes = 0;

  // Maximum size of training data passed to zstd's dictionary trainer. Using
  // zstd's dictionary trainer can achieve even better compression ratio
  // improvements than using `max_dict_bytes` alone.
  //
  // The training data will be used to generate a dictionary of max_dict_bytes.
  uint32_t zstd_max_train_bytes = 0;

  // Number of threads for parallel compression.
  // Parallel compression is enabled only if threads > 1.
  // THE FEATURE IS STILL EXPERIMENTAL
  //
  // This option is valid only when BlockBasedTable is used.
  //
  // When parallel compression is enabled, SST size file sizes might be
  // more inflated compared to the target size, because more data of unknown
  // compressed size is in flight when compression is parallelized. To be
  // reasonably accurate, this inflation is also estimated by using historical
  // compression ratio and current bytes inflight.
  uint32_t parallel_threads = 1;

  // When the compression options are set by the user, it will be set to "true".
  // For bottommost_compression_opts, to enable it, user must set enabled=true.
  // Otherwise, bottommost compression will use compression_opts as default
  // compression options.
  //
  // For compression_opts, if compression_opts.enabled=false, it is still
  // used as compression options for compression process.
  bool enabled = false;

  // Limit on data buffering when gathering samples to build a dictionary. Zero
  // means no limit. When dictionary is disabled (`max_dict_bytes == 0`),
  // enabling this limit (`max_dict_buffer_bytes != 0`) has no effect.
  //
  // In compaction, the buffering is limited to the target file size (see
  // `target_file_size_base` and `target_file_size_multiplier`) even if this
  // setting permits more buffering. Since we cannot determine where the file
  // should be cut until data blocks are compressed with dictionary, buffering
  // more than the target file size could lead to selecting samples that belong
  // to a later output SST.
  //
  // Limiting too strictly may harm dictionary effectiveness since it forces
  // RocksDB to pick samples from the initial portion of the output SST, which
  // may not be representative of the whole file. Configuring this limit below
  // `zstd_max_train_bytes` (when enabled) can restrict how many samples we can
  // pass to the dictionary trainer. Configuring it below `max_dict_bytes` can
  // restrict the size of the final dictionary.
  uint64_t max_dict_buffer_bytes = 0;

  // Use zstd trainer to generate dictionaries. When this option is set to true,
  // zstd_max_train_bytes of training data sampled from max_dict_buffer_bytes
  // buffered data will be passed to zstd dictionary trainer to generate a
  // dictionary of size max_dict_bytes.
  //
  // When this option is false, zstd's API ZDICT_finalizeDictionary() will be
  // called to generate dictionaries. zstd_max_train_bytes of training sampled
  // data will be passed to this API. Using this API should save CPU time on
  // dictionary training, but the compression ratio may not be as good as using
  // a dictionary trainer.
  bool use_zstd_dict_trainer = true;

  // ===> END options that can be set by deprecated configuration syntax <===
  // ===> Use compression_opts={level=6;strategy=7;} form for below opts <===

  // Essentially specifies a minimum acceptable compression ratio. A block is
  // stored uncompressed if the compressed block does not achieve this ratio,
  // because the downstream cost of decompression is not considered worth such
  // a small savings (if any).
  // However, the ratio is specified in a way that is efficient for checking.
  // An integer from 1 to 1024 indicates the maximum allowable compressed bytes
  // per 1KB of input, so the minimum acceptable ratio is 1024.0 / this value.
  // For example, for a minimum ratio of 1.5:1, set to 683. See SetMinRatio().
  // Default: abandon use of compression for a specific block or entry if
  // compressed by less than 12.5% (minimum ratio of 1.143:1).
  int max_compressed_bytes_per_kb = 1024 * 7 / 8;

  // ZSTD only.
  // Enable compression algorithm's checksum feature.
  // (https://github.com/facebook/zstd/blob/d857369028d997c92ff1f1861a4d7f679a125464/lib/zstd.h#L428)
  // Each compressed frame will have a 32-bit checksum attached. The checksum
  // computed from the uncompressed data and can be verified during
  // decompression.
  bool checksum = false;

  // A convenience function for setting max_compressed_bytes_per_kb based on a
  // minimum acceptable compression ratio (uncompressed size over compressed
  // size).
  void SetMinRatio(double min_ratio) {
    max_compressed_bytes_per_kb = static_cast<int>(1024.0 / min_ratio + 0.5);
  }
};

// Temperature of a file. Used to pass to FileSystem for a different
// placement and/or coding.
// Reserve some numbers in the middle, in case we need to insert new tier
// there.
enum class Temperature : uint8_t {
  kUnknown = 0,
  kHot = 0x04,
  kWarm = 0x08,
  kCold = 0x0C,
  kLastTemperature,
};

struct FileTemperatureAge {
  Temperature temperature = Temperature::kUnknown;
  uint64_t age = 0;
};

struct CompactionOptionsFIFO {
  // once the total sum of table files reaches this, we will delete the oldest
  // table file
  // Default: 1GB
  uint64_t max_table_files_size;

  // If true, try to do compaction to compact smaller files into larger ones.
  // Minimum files to compact follows options.level0_file_num_compaction_trigger
  // and compaction won't trigger if average compact bytes per del file is
  // larger than options.write_buffer_size. This is to protect large files
  // from being compacted again.
  // Default: false;
  bool allow_compaction = false;

  // DEPRECATED
  // When not 0, if the data in the file is older than this threshold, RocksDB
  // will soon move the file to warm temperature.
  uint64_t age_for_warm = 0;

  // EXPERIMENTAL
  // Age (in seconds) threshold for different file temperatures.
  // When not empty, each element specifies an age threshold `age` and a
  // temperature such that if all the data in a file is older than `age`,
  // RocksDB will compact the file to the specified `temperature`.
  //
  // Note:
  // - Flushed files will always have temperature kUnknown.
  // - Compaction output files will have temperature kUnknown by default, so
  //   only temperatures other than kUnknown needs to be specified.
  // - The elements should be in increasing order with respect to `age` field.
  //
  // Dynamically changeable through SetOptions() API, e.g.,
  //   SetOptions("compaction_options_fifo",
  //   "{file_temperature_age_thresholds={
  //    {age=10;temperature=kWarm}:{age=20;temperature=kCold}}}")
  // In this example, all files that are at least 20 seconds old will be
  // compacted and output files will have temperature kCold. All files that are
  // at least 10 seconds old but younger than 20 seconds will be compacted to
  // files with temperature kWarm.
  //
  // Default: empty
  std::vector<FileTemperatureAge> file_temperature_age_thresholds{};

  CompactionOptionsFIFO() : max_table_files_size(1 * 1024 * 1024 * 1024) {}
  CompactionOptionsFIFO(uint64_t _max_table_files_size, bool _allow_compaction)
      : max_table_files_size(_max_table_files_size),
        allow_compaction(_allow_compaction) {}
};

// The control option of how the cache tiers will be used. Currently rocksdb
// support block cache (volatile tier), secondary cache (non-volatile tier).
// In the future, we may add more caching layers.
enum class CacheTier : uint8_t {
  kVolatileTier = 0,
  kVolatileCompressedTier = 0x01,
  kNonVolatileBlockTier = 0x02,
};

enum UpdateStatus {     // Return status For inplace update callback
  UPDATE_FAILED = 0,    // Nothing to update
  UPDATED_INPLACE = 1,  // Value updated inplace
  UPDATED = 2,          // No inplace update. Merged value set
};

enum class PrepopulateBlobCache : uint8_t {
  kDisable = 0x0,    // Disable prepopulate blob cache
  kFlushOnly = 0x1,  // Prepopulate blobs during flush only
};

struct AdvancedColumnFamilyOptions {
  // The maximum number of write buffers that are built up in memory.
  // The default and the minimum number is 2, so that when 1 write buffer
  // is being flushed to storage, new writes can continue to the other
  // write buffer.
  // If max_write_buffer_number > 3, writing will be slowed down to
  // options.delayed_write_rate if we are writing to the last write buffer
  // allowed.
  //
  // Default: 2
  //
  // Dynamically changeable through SetOptions() API
  int max_write_buffer_number = 2;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.  If set to 1, then
  // all write buffers are flushed to L0 as individual files and this increases
  // read amplification because a get request has to check in all of these
  // files. Also, an in-memory merge may result in writing lesser
  // data to storage if there are duplicate records in each of these
  // individual write buffers.
  // If atomic flush is enabled (options.atomic_flush == true), then this
  // option will be sanitized to 1.
  // Default: 1
  int min_write_buffer_number_to_merge = 1;

  // DEPRECATED
  // The total maximum number of write buffers to maintain in memory including
  // copies of buffers that have already been flushed.  Unlike
  // max_write_buffer_number, this parameter does not affect flushing.
  // This parameter is being replaced by max_write_buffer_size_to_maintain.
  // If both parameters are set to non-zero values, this parameter will be
  // ignored.
  int max_write_buffer_number_to_maintain = 0;

  // The target number of write history bytes to hold in memory. Write history
  // comprises the latest write buffers (memtables). To reach the target, write
  // buffers that were most recently flushed to SST files may be retained in
  // memory.
  //
  // This controls the target amount of write history that will be available
  // in memory for conflict checking when Transactions are used.
  //
  // This target may be undershot when the CF first opens and has not recovered
  // or received enough writes to reach the target. After reaching the target
  // once, it is guaranteed to never undershoot again. That guarantee is
  // implemented by retaining flushed write buffers in-memory until the oldest
  // one can be trimmed without dropping below the target.
  //
  // Examples with `max_write_buffer_size_to_maintain` set to 32MB:
  //
  // - One mutable memtable of 64MB, one unflushed immutable memtable of 64MB,
  //   and zero flushed immutable memtables. Nothing trimmable exists.
  // - One mutable memtable of 16MB, zero unflushed immutable memtables, and
  //   one flushed immutable memtable of 64MB. Trimming is disallowed because
  //   dropping the earliest (only) flushed immutable memtable would result in
  //   write history of 16MB < 32MB.
  // - One mutable memtable of 24MB, one unflushed immutable memtable of 16MB,
  //   and one flushed immutable memtable of 16MB. The earliest (only) flushed
  //   immutable memtable is trimmed because without it we still have
  //   16MB + 24MB = 40MB > 32MB of write history.
  //
  // When using an OptimisticTransactionDB:
  // If this value is too low, some transactions may fail at commit time due
  // to not being able to determine whether there were any write conflicts.
  //
  // When using a TransactionDB:
  // If Transaction::SetSnapshot is used, TransactionDB will read either
  // in-memory write buffers or SST files to do write-conflict checking.
  // Increasing this value can reduce the number of reads to SST files
  // done for conflict detection.
  //
  // Setting this value to 0 will cause write buffers to be freed immediately
  // after they are flushed. If this value is set to -1,
  // 'max_write_buffer_number * write_buffer_size' will be used.
  //
  // Default:
  // If using a TransactionDB/OptimisticTransactionDB, the default value will
  // be set to the value of 'max_write_buffer_number * write_buffer_size'
  // if it is not explicitly set by the user.  Otherwise, the default is 0.
  int64_t max_write_buffer_size_to_maintain = 0;

  // Allows thread-safe inplace updates. If this is true, there is no way to
  // achieve point-in-time consistency using snapshot or iterator (assuming
  // concurrent updates). Hence iterator and multi-get will return results
  // which are not consistent as of any point-in-time.
  // Backward iteration on memtables will not work either.
  // If inplace_callback function is not set,
  //   Put(key, new_value) will update inplace the existing_value iff
  //   * key exists in current memtable
  //   * new sizeof(new_value) <= sizeof(existing_value)
  //   * existing_value for that key is a put i.e. kTypeValue
  // If inplace_callback function is set, check doc for inplace_callback.
  // Default: false.
  bool inplace_update_support = false;

  // Number of locks used for inplace update
  // Default: 10000, if inplace_update_support = true, else 0.
  //
  // Dynamically changeable through SetOptions() API
  size_t inplace_update_num_locks = 10000;

  // [experimental]
  // Used to activate or deactive the Mempurge feature (memtable garbage
  // collection). (deactivated by default). At every flush, the total useful
  // payload (total entries minus garbage entries) is estimated as a ratio
  // [useful payload bytes]/[size of a memtable (in bytes)]. This ratio is then
  // compared to this `threshold` value:
  //     - if ratio<threshold: the flush is replaced by a mempurge operation
  //     - else: a regular flush operation takes place.
  // Threshold values:
  //   0.0: mempurge deactivated (default).
  //   1.0: recommended threshold value.
  //   >1.0 : aggressive mempurge.
  //   0 < threshold < 1.0: mempurge triggered only for very low useful payload
  //   ratios.
  // [experimental]
  double experimental_mempurge_threshold = 0.0;

  // existing_value - pointer to previous value (from both memtable and sst).
  //                  nullptr if key doesn't exist
  // existing_value_size - pointer to size of existing_value).
  //                       nullptr if key doesn't exist
  // delta_value - Delta value to be merged with the existing_value.
  //               Stored in transaction logs.
  // merged_value - Set when delta is applied on the previous value.
  //
  // Applicable only when inplace_update_support is true,
  // this callback function is called at the time of updating the memtable
  // as part of a Put operation, lets say Put(key, delta_value). It allows the
  // 'delta_value' specified as part of the Put operation to be merged with
  // an 'existing_value' of the key in the database.
  //
  // If the merged value is smaller in size that the 'existing_value',
  // then this function can update the 'existing_value' buffer inplace and
  // the corresponding 'existing_value'_size pointer, if it wishes to.
  // The callback should return UpdateStatus::UPDATED_INPLACE.
  // In this case. (In this case, the snapshot-semantics of the rocksdb
  // Iterator is not atomic anymore).
  //
  // If the merged value is larger in size than the 'existing_value' or the
  // application does not wish to modify the 'existing_value' buffer inplace,
  // then the merged value should be returned via *merge_value. It is set by
  // merging the 'existing_value' and the Put 'delta_value'. The callback should
  // return UpdateStatus::UPDATED in this case. This merged value will be added
  // to the memtable.
  //
  // If merging fails or the application does not wish to take any action,
  // then the callback should return UpdateStatus::UPDATE_FAILED.
  //
  // Please remember that the original call from the application is Put(key,
  // delta_value). So the transaction log (if enabled) will still contain (key,
  // delta_value). The 'merged_value' is not stored in the transaction log.
  // Hence the inplace_callback function should be consistent across db reopens.
  //
  // RocksDB callbacks are NOT exception-safe. A callback completing with an
  // exception can lead to undefined behavior in RocksDB, including data loss,
  // unreported corruption, deadlocks, and more.
  //
  // Default: nullptr
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value) = nullptr;

  // Should really be called `memtable_bloom_size_ratio`. Enables a dynamic
  // Bloom filter in memtable to optimize many queries that must go beyond
  // the memtable. The size in bytes of the filter is
  // write_buffer_size * memtable_prefix_bloom_size_ratio.
  // * If prefix_extractor is set, the filter includes prefixes.
  // * If memtable_whole_key_filtering, the filter includes whole keys.
  // * If both, the filter includes both.
  // * If neither, the feature is disabled.
  //
  // If this value is larger than 0.25, it is sanitized to 0.25.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  double memtable_prefix_bloom_size_ratio = 0.0;

  // Enable whole key bloom filter in memtable. Note this will only take effect
  // if memtable_prefix_bloom_size_ratio is not 0. Enabling whole key filtering
  // can potentially reduce CPU usage for point-look-ups.
  //
  // Default: false (disabled)
  //
  // Dynamically changeable through SetOptions() API
  bool memtable_whole_key_filtering = false;

  // Page size for huge page for the arena used by the memtable. If <=0, it
  // won't allocate from huge page but from malloc.
  // Users are responsible to reserve huge pages for it to be allocated. For
  // example:
  //      sysctl -w vm.nr_hugepages=20
  // See linux doc Documentation/vm/hugetlbpage.txt
  // If there isn't enough free huge page available, it will fall back to
  // malloc.
  //
  // Dynamically changeable through SetOptions() API
  size_t memtable_huge_page_size = 0;

  // If non-nullptr, memtable will use the specified function to extract
  // prefixes for keys, and for each prefix maintain a hint of insert location
  // to reduce CPU usage for inserting keys with the prefix. Keys out of
  // domain of the prefix extractor will be insert without using hints.
  //
  // Currently only the default skiplist based memtable implements the feature.
  // All other memtable implementation will ignore the option. It incurs ~250
  // additional bytes of memory overhead to store a hint for each prefix.
  // Also concurrent writes (when allow_concurrent_memtable_write is true) will
  // ignore the option.
  //
  // The option is best suited for workloads where keys will likely to insert
  // to a location close the last inserted key with the same prefix.
  // One example could be inserting keys of the form (prefix + timestamp),
  // and keys of the same prefix always comes in with time order. Another
  // example would be updating the same key over and over again, in which case
  // the prefix can be the key itself.
  //
  // Default: nullptr (disabled)
  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor = nullptr;

  // Control locality of bloom filter probes to improve CPU cache hit rate.
  // This option now only applies to plaintable prefix bloom. This
  // optimization is turned off when set to 0, and positive number to turn
  // it on.
  // Default: 0
  uint32_t bloom_locality = 0;

  // size of one block in arena memory allocation.
  // If <= 0, a proper value is automatically calculated (usually 1/8 of
  // writer_buffer_size, rounded up to a multiple of 4KB, or 1MB which ever is
  // smaller).
  //
  // There are two additional restriction of the specified size:
  // (1) size should be in the range of [4096, 2 << 30] and
  // (2) be the multiple of the CPU word (which helps with the memory
  // alignment).
  //
  // We'll automatically check and adjust the size number to make sure it
  // conforms to the restrictions.
  //
  // Default: 0
  //
  // Dynamically changeable through SetOptions() API
  size_t arena_block_size = 0;

  // Different levels can have different compression policies. There
  // are cases where most lower levels would like to use quick compression
  // algorithms while the higher levels (which have more data) use
  // compression algorithms that have better compression but could
  // be slower. This array, if non-empty, should have an entry for
  // each level of the database; these override the value specified in
  // the previous field 'compression'.
  //
  // NOTICE if level_compaction_dynamic_level_bytes=true,
  // compression_per_level[0] still determines L0, but other elements
  // of the array are based on base level (the level L0 files are merged
  // to), and may not match the level users see from info log for metadata.
  // If L0 files are merged to level-n, then, for i>0, compression_per_level[i]
  // determines compaction type for level n+i-1.
  // For example, if we have three 5 levels, and we determine to merge L0
  // data to L4 (which means L1..L3 will be empty), then the new files go to
  // L4 uses compression type compression_per_level[1].
  // If now L0 is merged to L2. Data goes to L2 will be compressed
  // according to compression_per_level[1], L3 using compression_per_level[2]
  // and L4 using compression_per_level[3]. Compaction for each level can
  // change when data grows.
  //
  // NOTE: if the vector size is smaller than the level number, the undefined
  // lower level uses the last option in the vector, for example, for 3 level
  // LSM tree the following settings are the same:
  // {kNoCompression, kSnappyCompression}
  // {kNoCompression, kSnappyCompression, kSnappyCompression}
  //
  // Dynamically changeable through SetOptions() API
  std::vector<CompressionType> compression_per_level;

  // Number of levels for this database
  int num_levels = 7;

  // Soft limit on number of level-0 files. We start slowing down writes at this
  // point. A value <0 means that no writing slow down will be triggered by
  // number of files in level-0.
  //
  // Default: 20
  //
  // Dynamically changeable through SetOptions() API
  int level0_slowdown_writes_trigger = 20;

  // Maximum number of level-0 files.  We stop writes at this point.
  //
  // Default: 36
  //
  // Dynamically changeable through SetOptions() API
  int level0_stop_writes_trigger = 36;

  // Target file size for compaction.
  // target_file_size_base is per-file size for level-1.
  // Target file size for level L can be calculated by
  // target_file_size_base * (target_file_size_multiplier ^ (L-1))
  // For example, if target_file_size_base is 2MB and
  // target_file_size_multiplier is 10, then each file on level-1 will
  // be 2MB, and each file on level 2 will be 20MB,
  // and each file on level-3 will be 200MB.
  //
  // Default: 64MB.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t target_file_size_base = 64 * 1048576;

  // By default target_file_size_multiplier is 1, which means
  // by default files in different levels will have similar size.
  //
  // Dynamically changeable through SetOptions() API
  int target_file_size_multiplier = 1;

  // If true, RocksDB will pick target size of each level dynamically.
  // We will pick a base level b >= 1. L0 will be directly merged into level b,
  // instead of always into level 1. Level 1 to b-1 need to be empty.
  // We try to pick b and its target size so that
  // 1. target size is in the range of
  //   (max_bytes_for_level_base / max_bytes_for_level_multiplier,
  //    max_bytes_for_level_base]
  // 2. target size of the last level (level num_levels-1) equals to the max
  //    size of a level in the LSM (typically the last level).
  // At the same time max_bytes_for_level_multiplier is still satisfied.
  // Note that max_bytes_for_level_multiplier_additional is ignored with this
  // flag on.
  //
  // With this option on, from an empty DB, we make last level the base level,
  // which means merging L0 data into the last level, until it exceeds
  // max_bytes_for_level_base. And then we make the second last level to be
  // base level, to start to merge L0 data to second last level, with its
  // target size to be 1/max_bytes_for_level_multiplier of the last level's
  // extra size. After the data accumulates more so that we need to move the
  // base level to the third last one, and so on.
  //
  // For example, assume max_bytes_for_level_multiplier=10, num_levels=6,
  // and max_bytes_for_level_base=10MB.
  // Target sizes of level 1 to 5 starts with:
  // [- - - - 10MB]
  // with base level is level 5. Target sizes of level 1 to 4 are not applicable
  // because they will not be used.
  // Until the size of Level 5 grows to more than 10MB, say 11MB, we make
  // base target to level 4 and now the targets looks like:
  // [- - - 1.1MB 11MB]
  // While data are accumulated, size targets are tuned based on actual data
  // of level 5. When level 5 has 50MB of data, the target is like:
  // [- - - 5MB 50MB]
  // Until level 5's actual size is more than 100MB, say 101MB. Now if we keep
  // level 4 to be the base level, its target size needs to be 10.1MB, which
  // doesn't satisfy the target size range. So now we make level 3 the target
  // size and the target sizes of the levels look like:
  // [- - 1.01MB 10.1MB 101MB]
  // In the same way, while level 5 further grows, all levels' targets grow,
  // like
  // [- - 5MB 50MB 500MB]
  // Until level 5 exceeds 1000MB and becomes 1001MB, we make level 2 the
  // base level and make levels' target sizes like this:
  // [- 1.001MB 10.01MB 100.1MB 1001MB]
  // and go on...
  //
  // By doing it, we give max_bytes_for_level_multiplier a priority against
  // max_bytes_for_level_base, for a more predictable LSM tree shape. It is
  // useful to limit worse case space amplification.
  // If `allow_ingest_behind=true` or `preclude_last_level_data_seconds > 0`,
  // then the last level is reserved, and we will start filling LSM from the
  // second last level.
  //
  // With this option on, compaction is more adaptive to write traffic:
  // Compaction priority will take into account estimated bytes to be compacted
  // down to a level and favors compacting lower levels when there is a write
  // traffic spike (and hence more compaction debt). Refer to
  // https://github.com/facebook/rocksdb/wiki/Leveled-Compactio#option-level_compaction_dynamic_level_bytes-and-levels-target-size
  // for more detailed description. See more implementation detail in:
  // VersionStorageInfo::ComputeCompactionScore().
  //
  // With this option on, unneeded levels will be drained automatically:
  // Note that there may be excessive levels (where target level size is 0 when
  // computed based on this feature) in the LSM. This can happen after a user
  // migrates to turn this feature on or deletes a lot of data. This is
  // especially likely when a user migrates from leveled compaction with a
  // smaller multiplier or from universal compaction. RocksDB will gradually
  // drain these unnecessary levels by compacting files down the LSM. Smaller
  // number of levels should help to reduce read amplification.
  //
  // Migration to turn on this option:
  // - Before RocksDB v8.2, users are expected to do a full manual compaction
  //   and then restart DB to turn on this option.
  // - Since RocksDB v8.2, users can just restart DB with this option on, as
  //   long as num_levels is no smaller than number of non-empty levels in the
  //   LSM. Migration will be done automatically by RocksDB. See more in
  //   https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#migrating-from-level_compaction_dynamic_level_bytesfalse-to-level_compaction_dynamic_level_bytestrue
  //
  // Default: true
  bool level_compaction_dynamic_level_bytes = true;

  // Allows RocksDB to generate files that are not exactly the target_file_size
  // only for the non-bottommost files. Which can reduce the write-amplification
  // from compaction. The file size could be from 0 to 2x target_file_size.
  // Once enabled, non-bottommost compaction will try to cut the files align
  // with the next level file boundaries (grandparent level).
  //
  // Default: true
  bool level_compaction_dynamic_file_size = true;

  // Default: 10.
  //
  // Dynamically changeable through SetOptions() API
  double max_bytes_for_level_multiplier = 10;

  // Different max-size multipliers for different levels.
  // These are multiplied by max_bytes_for_level_multiplier to arrive
  // at the max-size of each level.
  // This option only applies to leveled compaction with
  // `level_compaction_dynamic_level_bytes = false`.
  //
  // Default: 1
  //
  // Dynamically changeable through SetOptions() API
  std::vector<int> max_bytes_for_level_multiplier_additional =
      std::vector<int>(num_levels, 1);

  // We try to limit number of bytes in one compaction to be lower than this
  // threshold. But it's not guaranteed.
  // Value 0 will be sanitized.
  //
  // Default: target_file_size_base * 25
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_compaction_bytes = 0;

  // When setting up compaction input files, we ignore the
  // `max_compaction_bytes` limit when pulling in input files that are entirely
  // within output key range.
  //
  // Default: true
  //
  // Dynamically changeable through SetOptions() API
  // We could remove this knob and always ignore the limit once it is proven
  // safe.
  bool ignore_max_compaction_bytes_for_input = true;

  // All writes will be slowed down to at least delayed_write_rate if estimated
  // bytes needed to be compaction exceed this threshold.
  //
  // Default: 64GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t soft_pending_compaction_bytes_limit = 64 * 1073741824ull;

  // All writes are stopped if estimated bytes needed to be compaction exceed
  // this threshold.
  //
  // Default: 256GB
  //
  // Dynamically changeable through SetOptions() API
  uint64_t hard_pending_compaction_bytes_limit = 256 * 1073741824ull;

  // The compaction style. Default: kCompactionStyleLevel
  CompactionStyle compaction_style = kCompactionStyleLevel;

  // If level compaction_style = kCompactionStyleLevel, for each level,
  // which files are prioritized to be picked to compact.
  // Default: kMinOverlappingRatio
  CompactionPri compaction_pri = kMinOverlappingRatio;

  // The options needed to support Universal Style compactions
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_universal", "{size_ratio=2;}")
  CompactionOptionsUniversal compaction_options_universal;

  // The options for FIFO compaction style
  //
  // Dynamically changeable through SetOptions() API
  // Dynamic change example:
  // SetOptions("compaction_options_fifo", "{max_table_files_size=100;}")
  CompactionOptionsFIFO compaction_options_fifo;

  // An iteration->Next() sequentially skips over keys with the same
  // user-key unless this option is set. This number specifies the number
  // of keys (with the same userkey) that will be sequentially
  // skipped before a reseek is issued.
  //
  // Default: 8
  //
  // Dynamically changeable through SetOptions() API
  uint64_t max_sequential_skip_in_iterations = 8;

  // This is a factory that provides MemTableRep objects.
  // Default: a factory that provides a skip-list-based implementation of
  // MemTableRep.
  std::shared_ptr<MemTableRepFactory> memtable_factory =
      std::shared_ptr<SkipListFactory>(new SkipListFactory);

  // Block-based table related options are moved to BlockBasedTableOptions.
  // Related options that were originally here but now moved include:
  //   no_block_cache
  //   block_cache
  //   block_cache_compressed (removed)
  //   block_size
  //   block_size_deviation
  //   block_restart_interval
  //   filter_policy
  //   whole_key_filtering
  // If you'd like to customize some of these options, you will need to
  // use NewBlockBasedTableFactory() to construct a new table factory.

  // This option allows user to collect their own interested statistics of
  // the tables.
  // Default: empty vector -- no user-defined statistics collection will be
  // performed.
  using TablePropertiesCollectorFactories =
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>;
  TablePropertiesCollectorFactories table_properties_collector_factories;

  // Maximum number of successive merge operations on a key in the memtable.
  //
  // When a merge operation is added to the memtable and the maximum number of
  // successive merges is reached, the value of the key will be calculated and
  // inserted into the memtable instead of the merge operation. This will
  // ensure that there are never more than max_successive_merges merge
  // operations in the memtable.
  //
  // Default: 0 (disabled)
  //
  // Dynamically changeable through SetOptions() API
  size_t max_successive_merges = 0;

  // This flag specifies that the implementation should optimize the filters
  // mainly for cases where keys are found rather than also optimize for keys
  // missed. This would be used in cases where the application knows that
  // there are very few misses or the performance in the case of misses is not
  // important.
  //
  // For now, this flag allows us to not store filters for the last level i.e
  // the largest level which contains data of the LSM store. For keys which
  // are hits, the filters in this level are not useful because we will search
  // for the data anyway. NOTE: the filters in other levels are still useful
  // even for key hit because they tell us whether to look in that level or go
  // to the higher level.
  //
  // Default: false
  bool optimize_filters_for_hits = false;

  // During flush or compaction, check whether keys inserted to output files
  // are in order.
  //
  // Default: true
  //
  // Dynamically changeable through SetOptions() API
  bool check_flush_compaction_key_order = true;

  // After writing every SST file, reopen it and read all the keys.
  // Checks the hash of all of the keys and values written versus the
  // keys in the file and signals a corruption if they do not match
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool paranoid_file_checks = false;

  // In debug mode, RocksDB runs consistency checks on the LSM every time the
  // LSM changes (Flush, Compaction, AddFile). When this option is true, these
  // checks are also enabled in release mode. These checks were historically
  // disabled in release mode, but are now enabled by default for proactive
  // corruption detection. The CPU overhead is negligible for normal mixed
  // operations but can slow down saturated writing. See
  // Options::DisableExtraChecks().
  // Default: true
  bool force_consistency_checks = true;

  // Measure IO stats in compactions and flushes, if true.
  //
  // Default: false
  //
  // Dynamically changeable through SetOptions() API
  bool report_bg_io_stats = false;

  // This option has different meanings for different compaction styles:
  //
  // Leveled: Non-bottom-level files with all keys older than TTL will go
  //    through the compaction process. This usually happens in a cascading
  //    way so that those entries will be compacted to bottommost level/file.
  //    The feature is used to remove stale entries that have been deleted or
  //    updated from the file system.
  //
  // FIFO: Files with all keys older than TTL will be deleted. TTL is only
  //    supported if option max_open_files is set to -1.
  //
  // Universal: users should only set the option `periodic_compaction_seconds`
  //    below instead. For backward compatibility, this option has the same
  //    meaning as `periodic_compaction_seconds`. See more in comments for
  //    `periodic_compaction_seconds` on the interaction between these two
  //    options.
  //
  // This option only supports block based table format for any compaction
  // style.
  //
  // unit: seconds. Ex: 1 day = 1 * 24 * 60 * 60
  // 0 means disabling.
  // UINT64_MAX - 1 (0xfffffffffffffffe) is special flag to allow RocksDB to
  // pick default.
  //
  // Default: 30 days if using block based table. 0 (disable) otherwise.
  //
  // Dynamically changeable through SetOptions() API
  // Note that dynamically changing this option only works for leveled and FIFO
  // compaction. For universal compaction, dynamically changing this option has
  // no effect, users should dynamically change `periodic_compaction_seconds`
  // instead.
  uint64_t ttl = 0xfffffffffffffffe;

  // This option has different meanings for different compaction styles:
  //
  // Leveled: files older than `periodic_compaction_seconds` will be picked up
  //    for compaction and will be re-written to the same level as they were
  //    before.
  //
  // FIFO: not supported. Setting this option has no effect for FIFO compaction.
  //
  // Universal: when there are files older than `periodic_compaction_seconds`,
  //    rocksdb will try to do as large a compaction as possible including the
  //    last level. Such compaction is only skipped if only last level is to
  //    be compacted and no file in last level is older than
  //    `periodic_compaction_seconds`. See more in
  //    UniversalCompactionBuilder::PickPeriodicCompaction().
  //    For backward compatibility, the effective value of this option takes
  //    into account the value of option `ttl`. The logic is as follows:
  //    - both options are set to 30 days if they have the default value.
  //    - if both options are zero, zero is picked. Otherwise, we take the min
  //    value among non-zero options values (i.e. takes the stricter limit).
  //
  // One main use of the feature is to make sure a file goes through compaction
  // filters periodically. Users can also use the feature to clear up SST
  // files using old format.
  //
  // A file's age is computed by looking at file_creation_time or creation_time
  // table properties in order, if they have valid non-zero values; if not, the
  // age is based on the file's last modified time (given by the underlying
  // Env).
  //
  // This option only supports block based table format for any compaction
  // style.
  //
  // unit: seconds. Ex: 7 days = 7 * 24 * 60 * 60
  //
  // Values:
  // 0: Turn off Periodic compactions.
  // UINT64_MAX - 1 (0xfffffffffffffffe) is special flag to allow RocksDB to
  // pick default.
  //
  // Default: 30 days if using block based table format + compaction filter +
  //  leveled compaction or block based table format + universal compaction.
  //  0 (disabled) otherwise.
  //
  // Dynamically changeable through SetOptions() API
  uint64_t periodic_compaction_seconds = 0xfffffffffffffffe;

  // If this option is set then 1 in N blocks are compressed
  // using a fast (lz4) and slow (zstd) compression algorithm.
  // The compressibility is reported as stats and the stored
  // data is left uncompressed (unless compression is also requested).
  uint64_t sample_for_compression = 0;

  // EXPERIMENTAL
  // The feature is still in development and is incomplete.
  // If this option is set, when creating the last level files, pass this
  // temperature to FileSystem used. Should be no-op for default FileSystem
  // and users need to plug in their own FileSystem to take advantage of it.
  //
  // Note: the feature is changed from `bottommost_temperature` to
  //  `last_level_temperature` which now only apply for the last level files.
  //  The option name `bottommost_temperature` is kept only for migration, the
  //  behavior is the same as `last_level_temperature`. Please stop using
  //  `bottommost_temperature` and will be removed in next release.
  //
  // Dynamically changeable through the SetOptions() API
  Temperature bottommost_temperature = Temperature::kUnknown;
  Temperature last_level_temperature = Temperature::kUnknown;

  // EXPERIMENTAL
  // When this field is set, all SST files without an explicitly set temperature
  // will be treated as if they have this temperature for file reading
  // accounting purpose, such as io statistics, io perf context.
  //
  // Not dynamically changeable, change it requires db restart.
  Temperature default_temperature = Temperature::kUnknown;

  // EXPERIMENTAL
  // The feature is still in development and is incomplete.
  // If this option is set, when data insert time is within this time range, it
  // will be precluded from the last level.
  // 0 means no key will be precluded from the last level.
  //
  // Note: when enabled, universal size amplification (controlled by option
  //  `compaction_options_universal.max_size_amplification_percent`) calculation
  //  will exclude the last level. As the feature is designed for tiered storage
  //  and a typical setting is the last level is cold tier which is likely not
  //  size constrained, the size amp is going to be only for non-last levels.
  //
  // Default: 0 (disable the feature)
  //
  // Not dynamically changeable, change it requires db restart.
  uint64_t preclude_last_level_data_seconds = 0;

  // EXPERIMENTAL
  // If this option is set, it will preserve the internal time information about
  // the data until it's older than the specified time here.
  // Internally the time information is a map between sequence number and time,
  // which is the same as `preclude_last_level_data_seconds`. But it won't
  // preclude the data from the last level and the data in the last level won't
  // have the sequence number zeroed out.
  // Internally, rocksdb would sample the sequence number to time pair and store
  // that in SST property "rocksdb.seqno.time.map". The information is currently
  // only used for tiered storage compaction (option
  // `preclude_last_level_data_seconds`).
  //
  // Note: if both `preclude_last_level_data_seconds` and this option is set, it
  //  will preserve the max time of the 2 options and compaction still preclude
  //  the data based on `preclude_last_level_data_seconds`.
  //  The higher the preserve_time is, the less the sampling frequency will be (
  //  which means less accuracy of the time estimation).
  //
  // Default: 0 (disable the feature)
  //
  // Not dynamically changeable, change it requires db restart.
  uint64_t preserve_internal_time_seconds = 0;

  // When set, large values (blobs) are written to separate blob files, and
  // only pointers to them are stored in SST files. This can reduce write
  // amplification for large-value use cases at the cost of introducing a level
  // of indirection for reads. See also the options min_blob_size,
  // blob_file_size, blob_compression_type, enable_blob_garbage_collection,
  // blob_garbage_collection_age_cutoff,
  // blob_garbage_collection_force_threshold, and blob_compaction_readahead_size
  // below.
  //
  // Default: false
  //
  // Dynamically changeable through the SetOptions() API
  bool enable_blob_files = false;

  // The size of the smallest value to be stored separately in a blob file.
  // Values which have an uncompressed size smaller than this threshold are
  // stored alongside the keys in SST files in the usual fashion. A value of
  // zero for this option means that all values are stored in blob files. Note
  // that enable_blob_files has to be set in order for this option to have any
  // effect.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t min_blob_size = 0;

  // The size limit for blob files. When writing blob files, a new file is
  // opened once this limit is reached. Note that enable_blob_files has to be
  // set in order for this option to have any effect.
  //
  // Default: 256 MB
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t blob_file_size = 1ULL << 28;

  // The compression algorithm to use for large values stored in blob files.
  // Note that enable_blob_files has to be set in order for this option to have
  // any effect.
  //
  // Default: no compression
  //
  // Dynamically changeable through the SetOptions() API
  CompressionType blob_compression_type = kNoCompression;

  // Enables garbage collection of blobs. Blob GC is performed as part of
  // compaction. Valid blobs residing in blob files older than a cutoff get
  // relocated to new files as they are encountered during compaction, which
  // makes it possible to clean up blob files once they contain nothing but
  // obsolete/garbage blobs. See also blob_garbage_collection_age_cutoff and
  // blob_garbage_collection_force_threshold below.
  //
  // Default: false
  //
  // Dynamically changeable through the SetOptions() API
  bool enable_blob_garbage_collection = false;

  // The cutoff in terms of blob file age for garbage collection. Blobs in
  // the oldest N blob files will be relocated when encountered during
  // compaction, where N = garbage_collection_cutoff * number_of_blob_files.
  // Note that enable_blob_garbage_collection has to be set in order for this
  // option to have any effect.
  //
  // Default: 0.25
  //
  // Dynamically changeable through the SetOptions() API
  double blob_garbage_collection_age_cutoff = 0.25;

  // If the ratio of garbage in the oldest blob files exceeds this threshold,
  // targeted compactions are scheduled in order to force garbage collecting
  // the blob files in question, assuming they are all eligible based on the
  // value of blob_garbage_collection_age_cutoff above. This option is
  // currently only supported with leveled compactions.
  // Note that enable_blob_garbage_collection has to be set in order for this
  // option to have any effect.
  //
  // Default: 1.0
  //
  // Dynamically changeable through the SetOptions() API
  double blob_garbage_collection_force_threshold = 1.0;

  // Compaction readahead for blob files.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  uint64_t blob_compaction_readahead_size = 0;

  // Enable blob files starting from a certain LSM tree level.
  //
  // For certain use cases that have a mix of short-lived and long-lived values,
  // it might make sense to support extracting large values only during
  // compactions whose output level is greater than or equal to a specified LSM
  // tree level (e.g. compactions into L1/L2/... or above). This could reduce
  // the space amplification caused by large values that are turned into garbage
  // shortly after being written at the price of some write amplification
  // incurred by long-lived values whose extraction to blob files is delayed.
  //
  // Default: 0
  //
  // Dynamically changeable through the SetOptions() API
  int blob_file_starting_level = 0;

  // The Cache object to use for blobs. Using a dedicated object for blobs and
  // using the same object for the block and blob caches are both supported. In
  // the latter case, note that blobs are less valuable from a caching
  // perspective than SST blocks, and some cache implementations have
  // configuration options that can be used to prioritize items accordingly (see
  // Cache::Priority and LRUCacheOptions::{high,low}_pri_pool_ratio).
  //
  // Default: nullptr (disabled)
  std::shared_ptr<Cache> blob_cache = nullptr;

  // Enable/disable prepopulating the blob cache. When set to kFlushOnly, BlobDB
  // will insert newly written blobs into the blob cache during flush. This can
  // improve performance when reading back these blobs would otherwise be
  // expensive (e.g. when using direct I/O or remote storage), or when the
  // workload has a high temporal locality.
  //
  // Default: disabled
  //
  // Dynamically changeable through the SetOptions() API
  PrepopulateBlobCache prepopulate_blob_cache = PrepopulateBlobCache::kDisable;

  // Enable memtable per key-value checksum protection.
  //
  // Each entry in memtable will be suffixed by a per key-value checksum.
  // This options determines the size of such checksums.
  //
  // It is suggested to turn on write batch per key-value
  // checksum protection together with this option, so that the checksum
  // computation is done outside of writer threads (memtable kv checksum can be
  // computed from write batch checksum) See
  // WriteOptions::protection_bytes_per_key for more detail.
  //
  // Default: 0 (no protection)
  // Supported values: 0, 1, 2, 4, 8.
  // Dynamically changeable through the SetOptions() API.
  uint32_t memtable_protection_bytes_per_key = 0;

  // UNDER CONSTRUCTION -- DO NOT USE
  // When the user-defined timestamp feature is enabled, this flag controls
  // whether the user-defined timestamps will be persisted.
  //
  // When it's false, the user-defined timestamps will be removed from the user
  // keys when data is flushed from memtables to SST files. Other places that
  // user keys can be persisted like file boundaries in file metadata and blob
  // files go through a similar process. There are two major motivations
  // for this flag:
  // 1) backward compatibility: if the user later decides to
  // disable the user-defined timestamp feature for the column family, these SST
  // files can be handled by a user comparator that is not aware of user-defined
  // timestamps.
  // 2) enable user-defined timestamp feature for an existing column family
  // while set this flag to be `false`: user keys in the newly generated SST
  // files are of the same format as the existing SST files.
  //
  // Currently only user comparator that formats user-defined timesamps as
  // uint64_t via using one of the RocksDB provided comparator
  // `ComparatorWithU64TsImpl` are supported.
  //
  // When setting this flag to `false`, users should also call
  // `DB::IncreaseFullHistoryTsLow` to set a cutoff timestamp for flush. RocksDB
  // refrains from flushing a memtable with data still above
  // the cutoff timestamp with best effort. If this cutoff timestamp is not set,
  // flushing continues normally.
  //
  // Users can do user-defined
  // multi-versioned read above the cutoff timestamp. When users try to read
  // below the cutoff timestamp, an error will be returned.
  //
  // Note that if WAL is enabled, unlike SST files, user-defined timestamps are
  // persisted to WAL even if this flag is set to `false`. The benefit of this
  // is that user-defined timestamps can be recovered with the caveat that users
  // should flush all memtables so there is no active WAL files before doing a
  // downgrade. In order to use WAL to recover user-defined timestamps, users of
  // this feature would want to set both `avoid_flush_during_shutdown` and
  // `avoid_flush_during_recovery` to be true.
  //
  // Note that setting this flag to false is not supported in combination with
  // atomic flush, or concurrent memtable write enabled by
  // `allow_concurrent_memtable_write`.
  //
  // Default: true (user-defined timestamps are persisted)
  // Not dynamically changeable, change it requires db restart and
  // only compatible changes are allowed.
  bool persist_user_defined_timestamps = true;

  // Enable/disable per key-value checksum protection for in memory blocks.
  //
  // Checksum is constructed when a block is loaded into memory and verification
  // is done for each key read from the block. This is useful for detecting
  // in-memory data corruption. Note that this feature has a non-trivial
  // negative impact on read performance. Different values of the
  // option have similar performance impact, but different memory cost and
  // corruption detection probability (e.g. 1 byte gives 255/256 chance for
  // detecting a corruption).
  //
  // Default: 0 (no protection)
  // Supported values: 0, 1, 2, 4, 8.
  // Dynamically changeable through the SetOptions() API.
  uint8_t block_protection_bytes_per_key = 0;

  // For leveled compaction, RocksDB may compact a file at the bottommost level
  // if it can compact away data that were protected by some snapshot.
  // The compaction reason in LOG for this kind of compactions is
  // "BottommostFiles". Usually such compaction can happen as soon as a
  // relevant snapshot is released. This option allows user to delay
  // such compactions. A file is qualified for "BottommostFiles" compaction
  // if it is at least "bottommost_file_compaction_delay" seconds old.
  //
  // Default: 0 (no delay)
  // Dynamically changeable through the SetOptions() API.
  uint32_t bottommost_file_compaction_delay = 0;

  ExternOptions* extern_options = nullptr;

  // Create ColumnFamilyOptions with default values for all fields
  AdvancedColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit AdvancedColumnFamilyOptions(const Options& options);

  // ---------------- OPTIONS NOT SUPPORTED ANYMORE ----------------
};

}  // namespace ROCKSDB_NAMESPACE
