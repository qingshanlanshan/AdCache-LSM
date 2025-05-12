#include "cache/heap_cache.h"

namespace ROCKSDB_NAMESPACE {
namespace heap_cache {
HeapCacheImpl::HeapCacheImpl(uint64_t cap)
    : logical_clock(0), capacity(cap), current_size(0) {}
HeapCacheImpl::~HeapCacheImpl() {
  for (auto item : pq) {
    delete item;
  }
}
bool HeapCacheImpl::heap_check() {
  return true;
  if (pq.size() != table.size()) {
    return false;
  }
  for (size_t i = 0; i < pq.size(); i++) {
    size_t left_child = 2 * i + 1;
    size_t right_child = 2 * i + 2;
    if (left_child < pq.size() && pq[i]->priority > pq[left_child]->priority) {
      return false;
    }
    if (right_child < pq.size() &&
        pq[i]->priority > pq[right_child]->priority) {
      return false;
    }
  }
  return true;
}

CacheItem* HeapCacheImpl::get(string key, bool is_kv) {
  (void)is_kv;
  assert(heap_check());
  if (table.find(key) != table.end()) {
    int idx = table[key];
    CacheItem* item = pq[idx];
    pq[idx]->frequency++;
    pq[idx]->priority = calculatePriority(*pq[idx]);
    heapify(idx);
    return item;
  }
  return nullptr;
}

size_t HeapCacheImpl::evict(uint64_t size) {
  size_t evicted_size = 0;
  while (current_size + size > capacity) {
    // for (auto item : pq) {
    //   std::cout << simple_hash(item->key) << " " << item->priority << std::endl;
    // }


    auto item = pop_heap();
    assert(current_size >= item->size);
    current_size -= item->size;
    if (item->priority > logical_clock) logical_clock = item->priority;
    // std::cout<<simple_hash(item->key)<<" evicted"<<std::endl;
    evicted_size += item->size;
    evict_count++;
    delete item;
  }
  return evicted_size;
}
void HeapCacheImpl::put(CacheItem* item) {
  assert(current_size <= capacity);
  evict(item->size);
  item->priority = calculatePriority(*item);
  pq.push_back(item);
  table[item->key] = pq.size() - 1;
  push_heap();
  current_size += item->size;
}

void HeapCacheImpl::set_next_victim(string key) {
  if (table.find(key) == table.end()) return;
  int idx = table[key];
  auto item = pq[idx];
  item->priority = 0;
  heapify(idx);
}

void HeapCacheImpl::report() {
  int n_kv = 0;
  int n_block = 0;
  for (auto item : pq) {
    if (item->type == kCacheKV)
      n_kv++;
    else
      n_block++;
  }
  std::cout << "Heap Cache:" << std::endl;
  std::cout << "item count: " << pq.size() << "\tkv: " << n_kv
            << "\tblock: " << n_block << std::endl;
  std::cout << "evict count: " << evict_count << std::endl;
}
void HeapCacheImpl::heap_swap(size_t i, size_t j) {
  if (i == j) return;
  std::swap(pq[i], pq[j]);
  table[pq[i]->key] = i;
  table[pq[j]->key] = j;
}
void HeapCacheImpl::move_down(size_t idx) {
  size_t left = 2 * idx + 1;
  size_t right = 2 * idx + 2;
  size_t smallest = idx;
  if (left < pq.size() && pq[left]->priority < pq[smallest]->priority)
    smallest = left;
  if (right < pq.size() && pq[right]->priority < pq[smallest]->priority)
    smallest = right;
  if (smallest != idx) {
    heap_swap(idx, smallest);
    move_down(smallest);
  }
}
void HeapCacheImpl::move_up(size_t idx) {
  if (idx == 0) return;
  size_t parent = (idx - 1) / 2;
  if (pq[parent]->priority > pq[idx]->priority) {
    heap_swap(idx, parent);
    move_up(parent);
  }
}
void HeapCacheImpl::heapify(size_t idx) {
  if (idx >= pq.size()) return;
  move_up(idx);
  move_down(idx);
}
CacheItem* HeapCacheImpl::pop_heap() {
  heap_swap(0, pq.size() - 1);
  table[pq[0]->key] = 0;
  table.erase(pq[pq.size() - 1]->key);
  CacheItem* item = pq.back();
  pq.pop_back();
  move_down(0);
  return item;
}
float HeapCacheImpl::calculatePriority(CacheItem& item) {
  if (item.size == 0) return 0;
  // gdsf
  return logical_clock + item.cost * item.frequency / item.size;
  // lru (heap)
  return logical_clock++;
}

CountMinSketch::CountMinSketch(int n_hash, int n_bucket)
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
void CountMinSketch::dump_sketch() {
  for (int i = 0; i < n_hash_; i++) {
    for (int j = 0; j < n_bucket_; j++) {
      std::cout << sketch[i][j] << " ";
    }
    std::cout << std::endl;
  }
}
void CountMinSketch::reset_sketch() {
  for (int i = 0; i < n_hash_; i++) {
    for (int j = 0; j < n_bucket_; j++) {
      sketch[i][j] = 0;
    }
  }
  sum_counter = 0;
  reseted = true;
}
void CountMinSketch::insert(std::string key, int count) {
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
size_t CountMinSketch::query(std::string key) {
  int freq = UINT8_MAX;
  for (int i = 0; i < n_hash_; i++) {
    uint32_t bucket = hash(i, key);
    freq = std::min(freq, (int)sketch[i][bucket]);
  }
  if (freq == UINT8_MAX) {
    return 0;
  }
  return freq;
}
uint32_t CountMinSketch::hash(int i, std::string key) {
  // hash to n_bucket
  uint32_t hash_value = hash_seed[i];
  for (auto c : key) {
    hash_value = hash_value * 33 + c;
  }
  return hash_value % n_bucket_;
}
int CountMinSketch::get_sum_counter() {
  if (!reseted)
    return sum_counter;
  else
    return INT32_MAX;
}

CacheItem* ELRUCacheImpl::get(string key, bool is_kv) {
  (void)is_kv;
  if (hash.find(key) == hash.end()) return nullptr;
  LinkedNode* node = hash[key];
  list.remove(node);
  list.append(node);
  node->item->frequency++;
  node->item->priority =
      node->item->cost * node->item->frequency / node->item->size;
  return node->item;
}
size_t ELRUCacheImpl::evict(uint64_t size) {
  size_t evicted_size = 0;
  while (current_size + size > capacity) {
    // print();
    

    if (window_size == 1) {
      auto node = list.pop();
      assert(node != nullptr);
      hash.erase(node->item->key);
      current_size -= node->item->size;
      // std::cout << simple_hash(node->item->key) << " evicted" << std::endl;
      evicted_size += node->item->size;
      delete node;
      evict_count++;
      continue;
    }
    LinkedNode* victim = nullptr;
    float min_priority = std::numeric_limits<float>::max();
    auto node = list.front();
    assert(node != nullptr);
    for (size_t i = 0; i < window_size && node != list.tail;
         i++, node = node->next) {
      if (node->item->priority < min_priority) {
        min_priority = node->item->priority;
        victim = node;
      }
    }
    assert(victim != nullptr);
    list.remove(victim);
    hash.erase(victim->item->key);
    current_size -= victim->item->size;
    delete victim;
    evicted_size += victim->item->size;
    evict_count++;
  }
  return evicted_size;
}
void ELRUCacheImpl::put(CacheItem* item) {
  assert(capacity > item->size);
  if (item->size == 0)
    item->priority = 0;
  else
    item->priority = item->cost * item->frequency / item->size;
  assert(hash.find(item->key) == hash.end());
  evict(item->size);
  LinkedNode* node = new LinkedNode();
  node->item = item;
  hash[item->key] = node;
  list.append(node);
  current_size += item->size;
  // print();
}
void ELRUCacheImpl::set_next_victim(string key) {
  assert(hash.find(key) != hash.end());
  // move to front
  LinkedNode* node = hash[key];
  list.remove(node);
  list.append_front(node);
  node->item->priority = 0;
}
size_t ELRUCacheImpl::get_capacity() const { return capacity; }
size_t ELRUCacheImpl::get_size() const { return current_size; }
void ELRUCacheImpl::modify_size(int diff) { current_size += diff; }
void ELRUCacheImpl::report() {
  int n_kv = 0;
  int n_block = 0;
  for (auto it : hash) {
    if (it.second->item->type == kCacheKV)
      n_kv++;
    else
      n_block++;
  }
  std::cout << "ELRU Cache:" << std::endl;
  std::cout << "item count: " << hash.size() << "\tkv: " << n_kv
            << "\tblock: " << n_block << std::endl;
  std::cout << "evict count: " << evict_count << std::endl;
}

LevelCacheImpl::LevelCacheImpl(uint64_t cap) : capacity(cap), current_size(0) {
  for (size_t i = 0; i < 3; i++) {
    levels.push_back(new HeapCacheImpl(capacity));
  }
}
LevelCacheImpl::~LevelCacheImpl() {
  for (auto level : levels) {
    delete level;
  }
}
CacheItem* LevelCacheImpl::get(string key, bool is_kv) {
  for (auto level : levels) {
    CacheItem* item = level->get(key, is_kv);
    if (item != nullptr) {
      return item;
    }
  }
  return nullptr;
}
size_t LevelCacheImpl::evict(uint64_t size) {
  auto evicted_size = levels[levels.size() - 1]->evict(size);
  current_size -= evicted_size;
  return evicted_size;
}
void LevelCacheImpl::put(CacheItem* item) {
  size_t level = item->level;
  if (level >= levels.size()) {
    level = levels.size() - 1;
  }
  if (current_size + item->size > capacity) {
    current_size -= level_evict(item->size);
  }
  levels[level]->put(item);
  current_size += item->size;
}
size_t LevelCacheImpl::get_capacity() const { return capacity; }
size_t LevelCacheImpl::get_size() const { return current_size; }
void LevelCacheImpl::modify_size(int diff) { current_size += diff; }
void LevelCacheImpl::report() {
  std::cout << "Level Cache:" << std::endl;
  for (size_t i = 0; i < levels.size(); i++) {
    std::cout << "Level " << i << ": ";
    levels[i]->report();
  }
}
void LevelCacheImpl::set_next_victim(string key) {
  for (auto level : levels) {
    level->set_next_victim(key);
  }
}
float LevelCacheImpl::calculatePriority(CacheItem& item) {
  return item.priority;
}
size_t LevelCacheImpl::level_evict(uint64_t size) {
  size_t evicted_size = 0;
  while (current_size + size > capacity) {
    float min_priority = std::numeric_limits<float>::max();
    int victim_level = 0;
    for (size_t i = 0; i < levels.size(); i++) {
      CacheItem* item = levels[i]->top();
      if (!item) continue;
      if (item->priority < min_priority) {
        min_priority = item->priority;
        victim_level = i;
      }
    }
    evicted_size = levels[victim_level]->evict(size);
    current_size -= evicted_size;
  }
  return evicted_size;
}
}  // namespace heap_cache
}  // namespace ROCKSDB_NAMESPACE