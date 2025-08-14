#pragma once

#include <limits>
#include <memory>
#include <random>
#include <vector>

#include "Utils.h"

class SkipList {
 public:
  struct Node {
    K key;
    std::shared_ptr<V> value;  // shared across containers
    bool next_key = false;     // indicate adjacent next key
    std::vector<Node*> forward;
    Node* prev;

    Node(K k, std::shared_ptr<V> v, int level)
        : key(k), value(std::move(v)), forward(level, nullptr), prev(nullptr) {}
  };

 private:
  static constexpr int MAX_LEVEL = 16;

  Node* head_;
  int level_;
  std::default_random_engine gen_;
  std::uniform_int_distribution<int> dist_;

 public:
  SkipList() : level_(1), dist_(0, 1) {
    // sentinel head; value not used
    head_ = new Node(std::numeric_limits<K>::min(), nullptr, MAX_LEVEL);
  }

  ~SkipList() {
    Node* curr = head_;
    while (curr) {
      Node* next = curr->forward[0];
      delete curr;
      curr = next;
    }
  }

  Node* find_node(K key) const {
    Node* x = head_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] && x->forward[i]->key < key) {
        x = x->forward[i];
      }
    }
    return x->forward[0];
  }

  // Insert with shared value (preferred for sharing with map)
  Node* insert(K key, std::shared_ptr<V> value) {
    std::vector<Node*> update(MAX_LEVEL);
    Node* x = head_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] && x->forward[i]->key < key) {
        x = x->forward[i];
      }
      update[i] = x;
    }
    x = x->forward[0];

    if (x && x->key == key) {
      x->value = std::move(value);  // re-point to shared value
      return x;
    } else {
      int lvl = random_level();
      if (lvl > level_) {
        for (int i = level_; i < lvl; ++i) update[i] = head_;
        level_ = lvl;
      }
      Node* new_node = new Node(key, std::move(value), lvl);
      for (int i = 0; i < lvl; ++i) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
      }
      // doubly-linked at level 0
      new_node->prev = update[0];
      if (new_node->forward[0]) new_node->forward[0]->prev = new_node;
      return new_node;
    }
  }

  // Convenience overload: constructs shared_ptr internally
  Node* insert(K key, const V& value) {
    return insert(key, std::make_shared<V>(value));
  }

  Node* insert_after(Node* node, K key, std::shared_ptr<V> value) {
    if (!node) return nullptr;
    Node* succ = node->forward[0];
    Node* new_node = new Node(key, std::move(value), 1);
    new_node->forward[0] = succ;
    node->forward[0] = new_node;
    new_node->prev = node;
    if (succ) succ->prev = new_node;
    return new_node;
  }

  // Convenience overload
  Node* insert_after(Node* node, K key, const V& value) {
    return insert_after(node, key, std::make_shared<V>(value));
  }

  Node* next(Node* node) const {
    return node ? node->forward[0] : nullptr;
  }

  Node* prev(Node* node) const {
    return node ? node->prev : nullptr;
  }

  bool remove(K key) {
    std::vector<Node*> update(MAX_LEVEL);
    Node* x = head_;
    for (int i = level_ - 1; i >= 0; --i) {
      while (x->forward[i] && x->forward[i]->key < key) {
        x = x->forward[i];
      }
      update[i] = x;
    }
    x = x->forward[0];
    if (!x || x->key != key) return false;

    for (int i = 0; i < level_; ++i) {
      if (update[i]->forward[i] != x) break;
      update[i]->forward[i] = x->forward[i];
    }
    if (x->forward[0]) x->forward[0]->prev = x->prev;
    delete x;
    while (level_ > 1 && head_->forward[level_ - 1] == nullptr) --level_;
    return true;
  }

  bool remove(Node* node) {
    if (!node) return false;
    return remove(node->key);
  }

 private:
  int random_level() {
    int lvl = 1;
    while (dist_(gen_) && lvl < MAX_LEVEL) ++lvl;
    return lvl;
  }
};
