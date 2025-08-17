#pragma once
#include <vector>
#include <unordered_map>
#include <cassert>
#include <iostream>
#include <algorithm>

// MinHeapDict implementation
// K: key type, V: value type (must be comparable with <)
template <typename K, typename V>
class HeapDict {
private:
    struct HeapEntry {
        K key;
        V value;
        size_t index;

        HeapEntry(K k, V v, size_t i) : key(std::move(k)), value(std::move(v)), index(i) {}

        bool operator<(const HeapEntry& other) const {
            return value < other.value;
        }

        friend std::ostream& operator<<(std::ostream& os, const HeapEntry& entry) {
            os << "(k=" << entry.key << ", v=" << entry.value << ", i=" << entry.index << ")";
            return os;
        }
    };

    std::unordered_map<K, HeapEntry*> table;
    std::vector<HeapEntry*> heap;

public:
    ~HeapDict() {
        for (auto* entry : heap) delete entry;
    }

    bool contains(const K& key) const {
        return table.count(key);
    }

    size_t size() const {
        return heap.size();
    }

    V& get(const K& key) {
        return table.at(key)->value;
    }

    void set(const K& key, const V& value) {
        if (contains(key)) update(key, value);
        else push(key, value);
    }

    void erase(const K& key) {
        remove(key);
    }

    V min() const {
        assert(!heap.empty());
        return heap[0]->value;
    }

    V popMin() {
        assert(!heap.empty());
        V val = heap[0]->value;
        remove(heap[0]->key);
        return val;
    }

private:
    void swap(HeapEntry* a, HeapEntry* b) {
        std::swap(a->index, b->index);
        heap[a->index] = a;
        heap[b->index] = b;
    }

    HeapEntry* parent(size_t index) {
        return heap[(index - 1) / 2];
    }

    HeapEntry* childLeft(size_t index) {
        size_t li = 2 * index + 1;
        return li < heap.size() ? heap[li] : nullptr;
    }

    HeapEntry* childRight(size_t index) {
        size_t ri = 2 * index + 2;
        return ri < heap.size() ? heap[ri] : nullptr;
    }

    void heapupify(size_t index) {
        auto* entry = heap[index];
        while (entry->index > 0 && entry->value < parent(entry->index)->value) {
            swap(entry, parent(entry->index));
        }
    }

    void heapify(size_t index) {
        auto* entry = heap[index];
        while (true) {
            auto* left = childLeft(entry->index);
            auto* right = childRight(entry->index);
            auto* minEntry = entry;
            if (left && *left < *minEntry) minEntry = left;
            if (right && *right < *minEntry) minEntry = right;

            if (minEntry != entry) swap(entry, minEntry);
            else break;
        }
    }

    void push(const K& key, const V& value) {
        assert(!contains(key));
        HeapEntry* entry = new HeapEntry(key, value, heap.size());
        table[key] = entry;
        heap.push_back(entry);
        heapupify(entry->index);
    }

    void update(const K& key, const V& value) {
        auto* entry = table.at(key);
        entry->value = value;
        heapupify(entry->index);
        heapify(entry->index);
    }

    void remove(const K& key) {
        assert(contains(key));
        auto* entry = table.at(key);
        HeapEntry* last = heap.back();
        if (entry != last) {
            swap(entry, last);
        }
        heap.pop_back();
        if (entry != last) {
            heapupify(last->index);
            heapify(last->index);
        }
        table.erase(key);
        delete entry;
    }
};
