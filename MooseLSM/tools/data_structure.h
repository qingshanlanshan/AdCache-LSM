#pragma once
#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <map>
#include <queue>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "tools/skiplist.h"

using namespace std;

class Cache {
 public:
  Cache() {}
  virtual ~Cache() {}
  virtual bool get(std::string& key, std::string& value) = 0;
  virtual void put(std::string& key, std::string& value) {}
  virtual void put(std::string& key, std::string& value, float cost) {
    put(key, value);
  }
  virtual void remove(std::string& key) = 0;
  virtual int get_capacity() = 0;
  virtual string name() { return "Cache"; }
};

struct LinkedNode {
  LinkedNode* prev = nullptr;
  LinkedNode* next = nullptr;
  std::string key;
  std::string value;
  double score;
};
class LinkedList {
 public:
  LinkedList() {
    head = new LinkedNode();
    tail = new LinkedNode();
    head->next = tail;
    tail->prev = head;
    len = 0;
  }
  void append(LinkedNode* node) {
    node->prev = tail->prev;
    node->next = tail;
    tail->prev->next = node;
    tail->prev = node;
    len++;
  }
  void remove(LinkedNode* node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    len--;
  }
  LinkedNode* pop() {
    LinkedNode* node = head->next;
    remove(node);
    return node;
  }
  LinkedNode* front() { return head->next; }

 private:
  LinkedNode* head;
  LinkedNode* tail;
  size_t len;
};

class LRUCache : public Cache {
 public:
  using Cache::put;
  LRUCache(int cap) : capacity(cap) {}
  bool get(std::string& key, std::string& value) override{
    if (capacity == 0) return false;
    if (hash.find(key) == hash.end()) {
      return false;
    }
    LinkedNode* node = hash[key];
    value = node->value;
    list.remove(node);
    list.append(node);
    return true;
  }

  void put(string& key, string& value) override {
    if (capacity == 0) return;
    if (hash.find(key) != hash.end()) {
      LinkedNode* node = hash[key];
      node->value = value;
      list.remove(node);
      list.append(node);
    } else {
      if ((int)hash.size() >= capacity) {
        LinkedNode* node = list.pop();
        hash.erase(node->key);
        delete node;
      }
      LinkedNode* node = new LinkedNode();
      node->key = key;
      node->value = value;
      hash[key] = node;
      list.append(node);
    }
  }

  int get_capacity() { return capacity; }

  int get_size() { return hash.size(); }

  void remove(std::string& key) override {
    if (hash.find(key) == hash.end()) return;
    LinkedNode* node = hash[key];
    list.remove(node);
    hash.erase(key);
    delete node;
  }

  string name() { return "LRU"; }

 protected:
  std::unordered_map<std::string, LinkedNode*> hash;
  LinkedList list;
  int capacity;
};

class ELRU : public LRUCache {
 public:
  using Cache::put;
  ELRU(int cap, int n) : LRUCache(cap), window_size(n) {}
  void put(std::string& key, std::string& value, float score) override {
    if (capacity == 0) return;
    if (hash.find(key) != hash.end()) {
      LinkedNode* node = hash[key];
      node->value = value;
      list.remove(node);
      list.append(node);
    } else {
      if ((int)hash.size() >= capacity) {
        auto node = list.front();
        auto worst_node = node;
        for (int i = 1; i < min(window_size, capacity); i++) {
          node = node->next;
          if (node->score < worst_node->score) worst_node = node;
        }
        hash.erase(worst_node->key);
        list.remove(worst_node);
      }
      LinkedNode* node = new LinkedNode();
      node->key = key;
      node->value = value;
      node->score = score;
      hash[key] = node;
      list.append(node);
    }
  }

  string name() { return "ELRU"; }

 private:
  int window_size = 1;
};

class CountMinSketch {
 public:
  CountMinSketch(int n_hash, int n_bucket)
      : n_hash_(n_hash), n_bucket_(n_bucket) {
    for (int i = 0; i < n_hash; i++) {
      hash_seed.push_back(rand());
    }
    sketch.resize(n_hash);
    for (int i = 0; i < n_hash; i++) {
      sketch[i].resize(n_bucket);
    }
    reset_sketch();
    reseted = false;
  }

  void dump_sketch() {
    for (int i = 0; i < n_hash_; i++) {
      for (int j = 0; j < n_bucket_; j++) {
        std::cout << sketch[i][j] << " ";
      }
      std::cout << endl;
    }
  }

  void reset_sketch() {
    // for (int i = 0; i < n_hash_; i++) {
    //   for (int j = 0; j < n_bucket_; j++) {
    //     sketch[i][j] = 0;
    //   }
    // }
    std::fill(sketch.begin(), sketch.end(), vector<uint8_t>(n_bucket_, 0));
    sum_counter = 0;
    reseted = true;
  }

  void halve() {
    for (int i = 0; i < n_hash_; i++) {
      for (int j = 0; j < n_bucket_; j++) {
        sketch[i][j] /= 2;
      }
    }
    std::fill(sketch.begin(), sketch.end(), vector<uint8_t>(n_bucket_, 0));
    sum_counter /= 2;
    reseted = true;
  }

  void insert(std::string key, int count) {
    bool overflow = false;
    for (int i = 0; i < n_hash_; i++) {
      uint32_t bucket = hash(i, key);
      if (count + (int)sketch[i][bucket] > 255) {
        overflow = true;
      }
      sketch[i][bucket] += count;
    }
    sum_counter++;
    if (overflow) {
      reset_sketch();
    }
  }

  // bool query(std::string key, float threshold = 0.1) {
  //   int res = UINT8_MAX;
  //   for (int i = 0; i < n_hash_; i++) {
  //     uint32_t bucket = hash(i, key);
  //     res = std::min(res, (int)sketch[i][bucket]);
  //   }
  //   if (res > get_sum_counter() * threshold) return true;
  //   return false;
  // }

  size_t query(std::string key) {
    int res = UINT8_MAX;
    for (int i = 0; i < n_hash_; i++) {
      uint32_t bucket = hash(i, key);
      res = std::min(res, (int)sketch[i][bucket]);
    }
    return res;
  }

  uint32_t hash(int i, std::string key) {
    // hash to n_bucket
    uint32_t hash_value = hash_seed[i];
    for (auto c : key) {
      hash_value = hash_value * 33 + c;
    }
    return hash_value % n_bucket_;
  }

  int get_sum_counter() {
    return sum_counter;
  }

 private:
  vector<vector<uint8_t>> sketch;
  vector<uint32_t> hash_seed;
  int n_hash_;
  int n_bucket_;
  int sum_counter;
  bool reseted;
};

class HeapCache : public Cache {
  using Cache::put;
  class CacheItem {
   public:
    string key;
    string value;
    float cost;
    size_t frequency;
    size_t size;
    // priority = logical_clock + cost*frequency/size
    float priority;

    bool operator<(const CacheItem& rhs) const {
      return priority < rhs.priority;
    }
    bool operator>(const CacheItem& rhs) const {
      return priority > rhs.priority;
    }
    CacheItem() {}
    CacheItem(float p) : priority(p) {}
  };

 private:
  std::vector<CacheItem*> pq;
  std::unordered_map<std::string, int> KVmap;

  float logical_clock;
  uint64_t capacity;
  uint64_t current_size;

  void move_down(size_t idx) {
    size_t left = 2 * idx + 1;
    size_t right = 2 * idx + 2;
    size_t smallest = idx;
    if (left < pq.size() && *pq[left] < *pq[smallest]) smallest = left;
    if (right < pq.size() && *pq[right] < *pq[smallest]) smallest = right;
    if (smallest != idx) {
      std::swap(pq[idx], pq[smallest]);
      KVmap[pq[idx]->key] = idx;
      KVmap[pq[smallest]->key] = smallest;
      move_down(smallest);
    }
  }
  void move_up(size_t idx) {
    if (idx == 0) return;
    size_t parent = (idx - 1) / 2;
    if (*pq[parent] > *pq[idx]) {
      std::swap(pq[parent], pq[idx]);
      KVmap[pq[parent]->key] = parent;
      KVmap[pq[idx]->key] = idx;
      move_up(parent);
    }
  }

  void heapify(size_t idx) {
    if (idx >= pq.size()) return;
    move_up(idx);
    move_down(idx);
  }

  void push_heap() {
    // heapify(pq.size() - 1);
    move_up(pq.size() - 1);
  }

  CacheItem* pop_heap() {
    std::swap(pq[0], pq[pq.size() - 1]);
    KVmap[pq[0]->key] = 0;
    KVmap.erase(pq[pq.size() - 1]->key);
    CacheItem* item = pq.back();
    pq.pop_back();
    move_down(0);
    return item;
  }
  float calculatePriority(const CacheItem& item) {
    // gdsf
    return logical_clock + item.cost * item.frequency / item.size;
    // lru
    // return chrono::steady_clock::now().time_since_epoch().count();
  }

 public:
  HeapCache(uint64_t cap) : logical_clock(0), capacity(cap), current_size(0) {}
  ~HeapCache() {
    unordered_map<int, int> freq;
    for (auto item : pq) {
      if (freq.find(item->cost) == freq.end())
        freq[item->cost] = 1;
      else
        freq[item->cost]++;
      delete item;
    }
    for (auto f : freq) {
      cout << "cost " << f.first << " frequency " << f.second << endl;
    }
  }
  bool get(string& key, std::string& value) override {
    if (KVmap.find(key) != KVmap.end()) {
      int idx = KVmap[key];
      value = pq[idx]->value;
      pq[idx]->frequency++;
      pq[idx]->priority = calculatePriority(*pq[idx]);
      heapify(idx);
      return true;
    }
    return false;
  }

  void put(std::string& key, std::string& value, float cost) override {
    CacheItem* item = new CacheItem();
    item->size = 1;
    item->cost = cost;
    item->frequency = 1;
    item->key = key;
    item->value = value;

    assert(current_size <= capacity);
    evict(item->size);
    item->priority = calculatePriority(*item);

    pq.push_back(item);
    KVmap[item->key] = pq.size() - 1;
    push_heap();
    current_size += item->size;
  }

  // by default, the cost could be the level of the entry
  void put(std::string& key, std::string& value, size_t size, float cost) {
    CacheItem* item = new CacheItem();
    item->size = size;
    item->cost = cost;
    item->frequency = 1;
    item->key = key;
    item->value = value;

    assert(current_size <= capacity);
    evict(item->size);
    item->priority = calculatePriority(*item);

    pq.push_back(item);
    KVmap[item->key] = pq.size() - 1;
    push_heap();
    current_size += item->size;
  }

  void evict(uint64_t size) {
    while (current_size + size > capacity) {
      auto item = pop_heap();
      current_size -= item->size;
      logical_clock = item->priority;
      delete item;
    }
  }

  void remove(std::string& key) {
    if (KVmap.find(key) == KVmap.end()) return;
    int idx = KVmap[key];
    swap(pq[idx], pq[pq.size() - 1]);
    KVmap[pq[idx]->key] = idx;
    KVmap.erase(pq[pq.size() - 1]->key);
    delete pq.back();
    pq.pop_back();
  }

  int get_capacity() { return capacity; }

  string name() { return "Heap"; }
};
#if OWN_SKIPLIST_IMPL
class RangeCache : public Cache
{
public:
using Cache::put;
  struct Node
  {
    string key;
    string value;
    Node *LRU_prev;
    Node *LRU_next;
  };
public:
  RangeCache(size_t cap)
  {
    skip_list = new SkipList();
    capacity = cap;
    lru_head = new Node();
    lru_tail = new Node();
    lru_head->LRU_next = lru_tail;
    lru_tail->LRU_prev = lru_head;
  }
  ~RangeCache()
  {
    delete skip_list;
    delete lru_head;
    delete lru_tail;
  }
  void lru_append(Node *node)
  {
    node->LRU_prev = lru_head;
    node->LRU_next = lru_head->LRU_next;
    lru_head->LRU_next->LRU_prev = node;
    lru_head->LRU_next = node;
  }
  void lru_remove(Node *node)
  {
    node->LRU_prev->LRU_next = node->LRU_next;
    node->LRU_next->LRU_prev = node->LRU_prev;
  }
  void access(Node *node)
  {
    lru_remove(node);
    lru_append(node);
  }
  void access(string key)
  {
    auto iter = hash.find(key);
    if (iter != hash.end())
    {
      auto node = iter->second;
      lru_remove(node);
      lru_append(node);
      return;
    }
  }
  void set_capacity(size_t cap)
  {
    capacity = cap;
    while (get_size() > capacity)
    {
      evict();
    }
  }
  size_t get_size()
  {
    assert(skip_list->size == hash.size());
    return skip_list->size;
  }
  void insert(string& key, string& value, bool insert_after = false)
  {
    auto iter = hash.find(key);
    if (iter != hash.end())
    {
      // iter->second->value = value;
      last_node = nullptr;
      access(iter->second);
      return;
    }
    if (insert_after && last_node) {
      last_node = skip_list->insert_after(last_node, key);
    } else {
      last_node = skip_list->insert(key);
    }
    auto node = new Node();
    node->key = key;
    node->value = value;
    lru_append(node);
    hash[key] = node;

    while (get_size() > capacity)
    {
      evict();
    }
  }
  void put(string& key, string &value)
  {
    insert(key, value);
  }
  void insert_range(vector<pair<string, string>> &range)
  {
    if (range.size() == 0)
      return;
    string start = range[0].first;
    string end = range[range.size() - 1].first;
    update_range(start, end);
    for (auto &r : range)
    {
      insert(r.first, r.second);
    }
    evict();
  }
  void update_range(string start, string end)
  {
    auto start_iter = ranges.lower_bound(start);
    auto end_iter = ranges.lower_bound(end);
    string new_start = start;
    string new_end = end;
    if (start_iter != ranges.end())
    {
      new_start = min(start_iter->second, start);
    }
    if (end_iter != ranges.end() && end_iter->second <= end)
    {
      new_end = max(end_iter->first, end);
    }
    for (auto iter = start_iter; iter != end_iter;)
    {
      ranges.erase(iter++);
    }
    ranges[new_end] = new_start;
  }
  void remove(string &key)
  {
    auto iter = hash.find(key);
    if (iter == hash.end())
    {
      return;
    }

    // update ranges
    auto item = ranges.lower_bound(key);
    if (item != ranges.end() && item->second <= key)
    {
      auto node = skip_list->search(key);
      assert(node != skip_list->head);
      string start = item->second;
      string end = item->first;
      if (start == end)
      {
        ranges.erase(item);
      }
      else if (start == key)
      {
        item->second = node->get_next()->data;
      }
      else if (end == key)
      {
        ranges.erase(item);
        ranges[node->get_prev()->data] = end;
      }
      else
      {
        ranges[key] = node->get_next()->data;
        ranges[node->get_prev()->data] = key;
      }
    }
    skip_list->remove(key);
    // remove from hash
    {
      auto node = iter->second;
      hash.erase(iter);
      lru_remove(node);
      delete node;
    }
  }
  void evict()
  {
    auto node = lru_tail->LRU_prev;
    assert(node != lru_head);
    remove(node->key);
  }
  void print()
  {
    skip_list->print();
    // print ranges
    for (auto &r : ranges)
    {
      cout << r.second << " -> " << r.first << endl;
    }
  }

  bool get(string &key, string &value)
  {
    auto iter = hash.find(key);
    if (iter == hash.end())
    {
      return false;
    }
    auto node = iter->second;
    value = node->value;
    access(node);
    return true;
  }

  string name() { return name_; }
  int get_capacity() { return (int)capacity; }

public:
  SkipList *skip_list;
  size_t capacity;
  string name_ = "Range";
  // range_end -> range_start
  map<string, string> ranges;
  // unordered_map<string, Node *> hash;
  robin_hood::unordered_map<string, Node *> hash;
  Node *lru_head;
  Node *lru_tail;

  SkipList::SkipListNode *last_node = nullptr;
};
#else
class RangeCache : public Cache
{
public:
  using Cache::put;
public:
  RangeCache(size_t cap)
  {
    skip_list = new SkipList(2 * cap + 200);
    capacity = cap;
    size = 0;
  }
  ~RangeCache()
  {
    delete skip_list;
  }
  void set_capacity(size_t cap)
  {
    capacity = cap;
    if (capacity != 0)
      evict();
    else
    {
      // fast path
      skip_list->reset();
      ranges.clear();
      size = 0;
    }
  }
  size_t get_size()
  {
    return size;
  }
  void insert(string& key, string& value, bool insert_after = false)
  {
    if (capacity == 0)
    {
      throw std::runtime_error("insert into capacity 0");
    }
    auto node = skip_list->find(key);
    if (node)
    {
      node->value = value;
      skip_list->LRU_append(node);
      last_node = node;
      return;
    }
    size++;
    if (insert_after && last_node) {
      last_node = skip_list->insert_after(last_node, key, value);
      // dont evict here
      // wait till update_range
    } else {
      last_node = skip_list->insert(key, value);
      evict();
    }
    skip_list->LRU_append(last_node);
  }
  void put(string& key, string& value) override
  {
    insert(key, value);
  }
  virtual void insert_range(vector<pair<string, string>> &range) 
  {
    if (range.size() == 0)
      return;
    string start = range[0].first;
    string end = range[range.size() - 1].first;
    update_range(start, end);
    for (auto &r : range)
    {
      insert(r.first, r.second);
    }
    evict();
  }
  void update_range(string& start, string& end)
  {
    if (capacity == 0)
    {
      throw std::runtime_error("insert into capacity 0");
    }
    auto start_iter = ranges.lower_bound(start);
    auto end_iter = ranges.lower_bound(end);
    FixedKey start_key(start);
    FixedKey end_key(end);
    FixedKey new_start = start_key;
    FixedKey new_end = end_key;
    if (start_iter != ranges.end())
    {
      new_start = min(start_iter->second, start_key);
    }
    if (end_iter != ranges.end() && end_iter->second <= end_key)
    {
      new_end = max(end_iter->first, end_key);
    }
    for (auto iter = start_iter; iter != end_iter;)
    {
      ranges.erase(iter++);
    }
    ranges[new_end] = new_start;
    evict();
  }
  void remove(string& key)
  {
    FixedKey target(key);
    // remove if exists
    auto node = skip_list->find(target);
    if (!node) return;
    remove(node);
  }
  void remove(Node* node)
  {
    auto target = node->key;
    // update ranges
    auto item = ranges.lower_bound(target);
    // if in range
    if (item != ranges.end() && item->second <= target) {
      auto start = item->second;
      auto end = item->first;

      if (start == end) {
        ranges.erase(item);
      } else if (start == target) {
        item->second = node->next()->key;
      } else if (end == target) {
        ranges.erase(item);
        ranges[node->prev()->key] = end;
      } else {
        // start -> key.prev
        ranges[node->prev()->key] = item->second;
        // key.next -> end
        item->second = node->next()->key;
      }
    }
    skip_list->remove(node);
  }
  void evict()
  {
    while (size > capacity)
    {
      if (!is_full)
      {
        is_full = true;
        cout<<"warmup done"<<endl;
      }
      auto node = skip_list->LRU_front();
      remove(node);
      size--;
    }
  }
  void print()
  {
    skip_list->print();
    // print ranges
    for (auto &r : ranges)
    {
      cout << r.second.to_string() << " -> " << r.first.to_string() << endl;
    }
  }

  bool get(string& key, string &value) override
  {
    auto node = skip_list->find(key);
    if (!node)
    {
      return false;
    }
    value = node->value;
    skip_list->LRU_append(node);
    return true;
  }

  string name() { return name_; }
  int get_capacity() { return (int)capacity; }

public:
  SkipList *skip_list;
  size_t capacity;
  size_t size;
  string name_ = "Range";
  // range_end -> range_start
  // map<string, string> ranges;
  map<FixedKey, FixedKey> ranges;
  Node* last_node = nullptr;
  bool is_full = false;
};
#endif
class RangeCache_LeCaR : public Cache {
  using Cache::put;

 public:
  struct SkipListNode {
    vector<SkipListNode*> next;
    vector<SkipListNode*> prev;
    SkipListNode* lru_prev;
    SkipListNode* lru_next;
    string key;
    string value;
    SkipListNode* get_next(int i = 0) { return next[i]; }
    SkipListNode* get_prev(int i = 0) { return prev[i]; }

    // for lfu
    size_t frequency;
    size_t idx;

    // for LeCaR
    float time;
  };
  // skip list + lru + lfu
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
    void insert(std::string key, std::string value, size_t frequency = 1) {
      SkipListNode* node = head;
      vector<SkipListNode*> update(level, nullptr);
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
      new_node->frequency = frequency;
      new_node->time = timestamp++;
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
      // lfu
      push_heap(new_node);

      size++;
    }
    void remove(std::string key) {
      SkipListNode* node = head;
      vector<SkipListNode*> update(level, nullptr);
      for (int i = level - 1; i >= 0; i--) {
        while (node->next[i] != tail && node->next[i]->key < key) {
          node = node->next[i];
        }
        update[i] = node;
      }
      if (node->next[0] && node->next[0]->key == key) {
        SkipListNode* target = node->next[0];
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
    }

    void print() {
      for (int i = level - 1; i >= 0; i--) {
        SkipListNode* node = head;
        cout << "level " << i << ": " << "h" << "->";
        while (node->next[i] != tail) {
          cout << node->next[i]->key << "->";
          node = node->next[i];
        }
        cout << "e" << endl;
      }
    }
    // heap
    void heap_swap(size_t i, size_t j) {
      heap[i]->idx = j;
      heap[j]->idx = i;
      std::swap(heap[i], heap[j]);
    }

    void move_down(size_t idx) {
      size_t left = 2 * idx + 1;
      size_t right = 2 * idx + 2;
      size_t smallest = idx;
      if (left < heap.size() && heap[left]->frequency < heap[smallest]->frequency) smallest = left;
      if (right < heap.size() && heap[right]->frequency < heap[smallest]->frequency) smallest = right;
      if (smallest != idx) {
        heap_swap(idx, smallest);
        move_down(smallest);
      }
    }

    void move_up(size_t idx) {
      if (idx == 0) return;
      size_t parent = (idx - 1) / 2;
      if (heap[parent]->frequency > heap[idx]->frequency) {
        heap_swap(parent, idx);
        move_up(parent);
      }
    }

    void heapify(size_t idx) {
      if (idx >= heap.size()) return;
      move_up(idx);
      move_down(idx);
    }

    void push_heap(SkipListNode* node) {
      node->idx = heap.size();
      heap.push_back(node);
      move_up(heap.size() - 1);
    }

    SkipListNode* pop_heap() {
      heap_swap(0, heap.size() - 1);
      SkipListNode* node = heap.back();
      heap.pop_back();
      move_down(0);
      return node;
    }

    SkipListNode* peek_heap() {
      if (heap.size() == 0) return nullptr;
      return heap[0];
    }

    SkipListNode* remove_heap(size_t idx) {
      assert (idx < heap.size());
      SkipListNode* node = heap[idx];
      heap_swap(idx, heap.size() - 1);
      heap.pop_back();
      move_down(idx);
      return node;
    }
    SkipListNode* head;
    SkipListNode* tail;
    size_t level;
    // old
    SkipListNode* lru_head;
    // new
    SkipListNode* lru_tail;
    size_t size;
    // for lfu
    std::vector<SkipListNode*> heap;

    size_t timestamp = 0;
  };
  // lru + hashmap, key only
  struct History {
    struct HistoryItem {
      string key;
      size_t freq;
      size_t time;
      HistoryItem* prev;
      HistoryItem* next;
    };
    History() {
      head = new HistoryItem();
      tail = new HistoryItem();
      head->next = tail;
      tail->prev = head;
    }
    ~History() {
      HistoryItem* node = head->next;
      while (node != tail) {
        HistoryItem* next = node->next;
        delete node;
        node = next;
      }
      delete head;
      delete tail;
    }
    void append(string key, size_t freq) {
      HistoryItem* node = new HistoryItem();
      node->key = key;
      node->freq = freq;

      node->prev = tail->prev;
      node->next = tail;
      tail->prev->next = node;
      tail->prev = node;

      hash[key] = node;
    }
    void remove(HistoryItem* node) {
      node->prev->next = node->next;
      node->next->prev = node->prev;
      hash.erase(node->key);
      delete node;
    }

    void pop() {
      if (head->next == tail) return;
      remove(head->next);
    }

    size_t get_size() { return hash.size(); }

    HistoryItem* get_item(string key) {
      if (hash.find(key) == hash.end()) return nullptr;
      return hash[key];
    }

    HistoryItem* head;
    HistoryItem* tail;
    unordered_map<string, HistoryItem*> hash;
  };
 public:
  RangeCache_LeCaR(size_t cap) {
    skip_list = new SkipList();
    capacity = cap;
    size = 0;
    discount_rate = pow(0.005, 1.0 / cap);
  }
  ~RangeCache_LeCaR() { delete skip_list; }
  size_t get_size() { return skip_list->size; }

  // miss
  void insert(string& key, string& value) {
    size_t freq = 0;
    History::HistoryItem* item;
    if ((item = lru_hist.get_item(key))) {
      freq = item->freq;
      float reward_lru = -pow(discount_rate, skip_list->timestamp - item->time);
      adjust_weights(reward_lru, 0);
      lru_hist.remove(item);
    }
    else if ((item = lfu_hist.get_item(key))) {
      freq = item->freq;
      float reward_lfu = -pow(discount_rate, skip_list->timestamp - item->time);
      adjust_weights(0, reward_lfu);
      lfu_hist.remove(item);
    }

    skip_list->insert(key, value, ++freq);
    while (get_size() > capacity) {
      evict();
    }
  }

  void put(string& key, string& value) {
    insert(key, value);
  }

  void insert_range(vector<pair<string, string>>& range) {
    if (range.size() == 0) return;
    string start = range[0].first;
    string end = range[range.size() - 1].first;
    
    auto start_iter = ranges.lower_bound(start);
    auto end_iter = ranges.lower_bound(end);
    string new_start = start;
    string new_end = end;
    if (start_iter != ranges.end()) {
      new_start = min(start_iter->second, start);
    }
    if (end_iter != ranges.end() && end_iter->second <= end) {
      new_end = max(end_iter->first, end);
    }
    for (auto iter = start_iter; iter != end_iter;) {
      ranges.erase(iter++);
    }
    ranges[new_end] = new_start;

    for (auto& r : range) {
      insert(r.first, r.second);
    }
  }

  void remove(string& key) {
    auto node = skip_list->search(key);
    assert(node && node->key == key);

    skip_list->lru_remove(node);
    skip_list->remove_heap(node->idx);
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
    auto lru_node = skip_list->lru_head->lru_next;
    auto lfu_node = skip_list->peek_heap();
    SkipListNode* victim = nullptr;
    assert(lru_node && lfu_node && lru_node != skip_list->lru_tail);
    bool choice = use_lru();
    if (lru_node == lfu_node)
    {
      victim = lru_node;
    }
    else if (choice)
    {
      victim = lru_node;
      lru_hist.append(victim->key, victim->frequency);
      if (lru_hist.get_size() > capacity)
      {
        lru_hist.pop();
      }
    }
    else
    {
      victim = lfu_node;
      lfu_hist.append(victim->key, victim->frequency);
      if (lfu_hist.get_size() > capacity)
      {
        lfu_hist.pop();
      }
    }

    remove(victim->key);
  }

  void print() {
    skip_list->print();
    // print ranges
    for (auto& r : ranges) {
      cout << r.second << " -> " << r.first << endl;
    }
  }
  SkipListNode* search(string key) { 
    return skip_list->search(key); 
  }

  bool get(string& key, string& value) {
    auto node = skip_list->search(key);
    if (!node) {
      return false;
    }
    if (node->key != key) {
      return false;
    }
    access(node);
    value = node->value;
    return true;
  }

  // hit
  void access(SkipListNode* node) {
    skip_list->lru_remove(node);
    skip_list->lru_append(node);
    node->frequency++;
    skip_list->heapify(node->idx);
    node->time = skip_list->timestamp++;
  }

  void adjust_weights(float lru_reward, float lfu_reward) {
    weights[0] *= exp(learning_rate * lru_reward);
    weights[1] *= exp(learning_rate * lfu_reward);
    float sum = weights[0] + weights[1];
    weights[0] /= sum;
    weights[1] /= sum;
    if (weights[0] < 0.01)
    {
      weights[0] = 0.01;
      weights[1] = 0.99;
    }
    if (weights[1] < 0.01)
    {
      weights[0] = 0.99;
      weights[1] = 0.01;
    }
  }
  
  string name() { return name_; }

  int get_capacity() { return (int)capacity; }

  bool use_lru() {
    return rand() % 100 < weights[0] * 100;
  }

 public:
  SkipList* skip_list;
  size_t capacity;
  size_t size;
  string name_ = "LeCaR";
  // range_end -> range_start
  map<string, string> ranges;
  History lru_hist;
  History lfu_hist;

  // learning
  float initial_weight = 0.5;

  float learning_rate = 0.45;

  float discount_rate;

  vector<float> weights{0.5, 0.5};
};

#if OWN_SKIPLIST_IMPL
class RLCache : public RangeCache {
public:
using Cache::put;
  RLCache(size_t cap) : RangeCache(cap) {
  }
  ~RLCache() {
  }
  void put(std::string& key, std::string& value) override {
    if ((int)get_size() < get_capacity()) 
    {
      RangeCache::put(key, value);
      return;
    }
    map[key]++;
    auto lru_victim = lru_tail->LRU_prev;
    if (map.find(lru_victim->key) == map.end() || map[lru_victim->key] < map[key]) {
      RangeCache::put(key, value);
    }
    if (map[key] > 8)
    {
      for (auto& pair : map)
      {
        pair.second /= 2;
      }
    }
  }
private:
  // CountMinSketch *sketch;
  unordered_map<string, size_t> map;
};
#else
class RLCache : public RangeCache {
 public:
  using Cache::put;
  RLCache(size_t cap) : RangeCache(cap) {}
  ~RLCache() {}
  void put(std::string& key, std::string& value, float threshold = 0) override {
    if (get_capacity() == 0) {
      return;
    }
    if ((int)get_size() < get_capacity()) {
      RangeCache::put(key, value);
      return;
    }
    map[key]++;
    map_counter++;
    if (threshold > 0 && threshold < 0.2)
    {
      if (map[key] > threshold * map_counter) {
        RangeCache::put(key, value);
      }
    }
    else{
      auto lru_victim = skip_list->LRU_front();
      if (map.find(lru_victim->key) == map.end() ||
          map[lru_victim->key] < map[key]) {
        RangeCache::put(key, value);
      }
    }
    
    if (map[key] > 8) {
      for (auto& pair : map) {
        pair.second /= 2;
      }
    }
  }

  void insert_range(vector<pair<string, string>> &range) override {
    if (get_capacity() == 0) {
      return;
    }
    bool insert_check = false;
    if ((int)get_size() < get_capacity()) insert_check = true;
    else {
      map[range[0].first]++;
      auto lru_victim = skip_list->LRU_front();
      if (map.find(lru_victim->key) == map.end() ||
          map[lru_victim->key] < map[range[0].first]) {
        insert_check = true;
      }
    }
    if (insert_check) {
      for (size_t i = 0; i < range.size(); i++) {
        RangeCache::insert(range[i].first, range[i].second, (bool)i);
      }
      RangeCache::update_range(range[0].first, range[range.size() - 1].first);
    }
    if (map[range[0].first] > 8) {
      for (auto& pair : map) {
        pair.second /= 2;
      }
    }
  }
 private:
  robin_hood::unordered_map<FixedKey, size_t> map;
  size_t map_counter = 0;
};
#endif