#pragma once
#include <unordered_map>
#include <cassert>
#include <iostream>

template <typename K, typename V>
class DequeDict {
private:
    struct Node {
        K key;
        V value;
        Node* prev;
        Node* next;

        Node(K k, V v) : key(std::move(k)), value(std::move(v)), prev(nullptr), next(nullptr) {}
    };

    std::unordered_map<K, Node*> table;
    Node* head = nullptr;
    Node* tail = nullptr;

public:
    DequeDict() = default;

    ~DequeDict() {
        while (head) {
            Node* next = head->next;
            delete head;
            head = next;
        }
    }

    // Insert or update
    void set(const K& key, const V& value) {
        if (table.count(key)) {
            update(key, value);
        } else {
            push(key, value);
        }
    }

    // Get by key
    V& get(const K& key) {
        return table.at(key)->value;
    }

    // Check existence
    bool contains(const K& key) const {
        return table.count(key);
    }

    // Size
    size_t size() const {
        return table.size();
    }

    // Delete by key
    void erase(const K& key) {
        remove(key);
    }

    // Get and remove first (LRU)
    V popFirst() {
        assert(head);
        V val = head->value;
        remove(head->key);
        return val;
    }

    // Get and remove last (MRU)
    V popLast() {
        assert(tail);
        V val = tail->value;
        remove(tail->key);
        return val;
    }

    // Access first/last without popping
    V& first() {
        assert(head);
        return head->value;
    }

    V& last() {
        assert(tail);
        return tail->value;
    }

    // Push to front (asserts not existing)
    void pushFirst(const K& key, const V& value) {
        assert(!table.count(key));
        Node* node = new Node(key, value);
        node->next = head;
        if (head) head->prev = node;
        head = node;
        if (!tail) tail = node;
        table[key] = node;
    }

private:
    void push(const K& key, const V& value) {
        Node* node = new Node(key, value);
        node->prev = tail;
        if (tail) tail->next = node;
        tail = node;
        if (!head) head = node;
        table[key] = node;
    }

    void update(const K& key, const V& value) {
        remove(key);
        push(key, value);
    }

    void remove(const K& key) {
        auto it = table.find(key);
        assert(it != table.end());
        Node* node = it->second;
        if (node->prev) node->prev->next = node->next;
        else head = node->next;
        if (node->next) node->next->prev = node->prev;
        else tail = node->prev;
        delete node;
        table.erase(it);
    }

public:
    // Iterator
    class Iterator {
        Node* current;

    public:
        explicit Iterator(Node* node) : current(node) {}

        Iterator& operator++() {
            current = current->next;
            return *this;
        }

        bool operator!=(const Iterator& other) const {
            return current != other.current;
        }

        V& operator*() const {
            return current->value;
        }
    };

    Iterator begin() const { return Iterator(head); }
    Iterator end() const { return Iterator(nullptr); }
};
