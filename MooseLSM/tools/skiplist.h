#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include "tools/robin_hood.h"
using namespace std;
#define OWN_SKIPLIST_IMPL 0
#if OWN_SKIPLIST_IMPL
struct SkipList
{
    struct SkipListNode
    {
        vector<SkipListNode *> next;
        vector<SkipListNode *> prev;
        string data;
        SkipListNode *get_next(int i = 0) { return next[i]; }
        SkipListNode *get_prev(int i = 0) { return prev[i]; }
    };
    SkipList()
    {
        head = new SkipListNode();
        tail = new SkipListNode();
        head->next.push_back(tail);
        tail->prev.push_back(head);
        level = 1;
        size = 0;
    }
    ~SkipList()
    {
        SkipListNode *node = head->get_next();
        while (node != tail)
        {
            SkipListNode *next = node->get_next();
            delete node;
            node = next;
        }
        delete head;
        delete tail;
    }
    // find the first node whose data >= data
    // if none, return tail
    SkipListNode *search(string data)
    {
        SkipListNode *node = head;
        assert(level == node->next.size());
        for (int i = level - 1; i >= 0; i--)
        {
            while (node->next[i] != tail && node->next[i]->data <= data)
            {
                node = node->next[i];
            }
        }
        if (node->data == data)
        {
            return node;
        }
        else {
            return node->next[0];
        }
    }
    SkipListNode* insert(std::string data)
    {
        SkipListNode *node = head;
        vector<SkipListNode *> update(level, nullptr);
        for (int i = level - 1; i >= 0; i--)
        {
            while (node->next[i] != tail && node->next[i]->data < data)
            {
                node = node->next[i];
            }
            update[i] = node;
        }
        if (node->next[0] && node->next[0]->data == data)
        {
            // already exist
            return node->next[0];
        }
        SkipListNode *new_node = new SkipListNode();
        new_node->data = data;
        size_t new_level = 1;
        while (rand() % 2 == 0)
        {
            new_level++;
        }
        if (new_level > level)
        {
            for (size_t i = level; i < new_level; i++)
            {
                update.push_back(head);
                head->next.push_back(tail);
                tail->prev.push_back(head);
            }
            level = new_level;
        }
        for (size_t i = 0; i < new_level; i++)
        {
            new_node->next.push_back(update[i]->next[i]);
            new_node->prev.push_back(update[i]);
            update[i]->next[i]->prev[i] = new_node;
            update[i]->next[i] = new_node;
        }
        size++;
        return new_node;
    }
    SkipListNode* insert_after(SkipListNode* node, std::string data)
    {
        if (node == nullptr)
        {
            return insert(data);
        }
        auto next_node = node->next[0];
        if (next_node->data == data)
        {
            return next_node;
        }
        SkipListNode *new_node = new SkipListNode();
        new_node->data = data;
        new_node->next.push_back(next_node);
        new_node->prev.push_back(node);
        node->next[0] = new_node;
        next_node->prev[0] = new_node;
        size++;
        return new_node;
    }
    void remove(std::string data)
    {
        SkipListNode *node = head;
        vector<SkipListNode *> update(level, nullptr);
        for (int i = level - 1; i >= 0; i--)
        {
            while (node->next[i] != tail && node->next[i]->data < data)
            {
                node = node->next[i];
            }
            update[i] = node;
        }
        if (node->next[0] && node->next[0]->data == data)
        {
            SkipListNode *target = node->next[0];
            // skip list
            for (size_t i = 0; i < level; i++)
            {
                if (update[i]->next[i] == target)
                {
                    update[i]->next[i] = target->next[i];
                    target->next[i]->prev[i] = update[i];
                }
            }
            delete target;
            size--;
        }
    }
    void print()
    {
        for (int i = level - 1; i >= 0; i--)
        {
            SkipListNode *node = head;
            cout << "level " << i << ": " << "HEAD" << "->";
            while (node->next[i] != tail)
            {
                cout << node->next[i]->data << "->";
                node = node->next[i];
            }
            cout << "TAIL" << endl;
        }
    }
    SkipListNode *head;
    SkipListNode *tail;
    size_t level;
    size_t size;
};
#else
constexpr int MAX_LEVEL = 16;

// ------------------- FixedKey -------------------
struct FixedKey {
    char data[16];

    // --------- Comparisons ----------
    int cmp(const FixedKey& other) const {
        return std::memcmp(data, other.data, sizeof(data));
    }

    bool operator==(const FixedKey& other) const { return cmp(other) == 0; }
    bool operator!=(const FixedKey& other) const { return cmp(other) != 0; }
    bool operator< (const FixedKey& other) const { return cmp(other) <  0; }
    bool operator> (const FixedKey& other) const { return cmp(other) >  0; }
    bool operator<=(const FixedKey& other) const { return cmp(other) <= 0; }
    bool operator>=(const FixedKey& other) const { return cmp(other) >= 0; }

    // --------- Constructors ----------
    FixedKey() {
        std::memset(data, 0, sizeof(data));
    }

    FixedKey(const char* str) {
        std::memset(data, 0, sizeof(data));
        std::memcpy(data, str, std::min(sizeof(data), std::strlen(str)));
    }

    FixedKey(const std::string& str) {
        std::memset(data, 0, sizeof(data));
        std::memcpy(data, str.data(), std::min(sizeof(data), str.size()));
    }

    // --------- Debug / Conversion ----------
    std::string to_string() const {
        return std::string(data, sizeof(data));
    }

    friend std::ostream& operator<<(std::ostream& os, const FixedKey& key) {
        return os << key.to_string();
    }
};

// --------- Hash Support ----------
namespace std {
    template <>
    struct hash<FixedKey> {
        size_t operator()(const FixedKey& k) const {
            size_t h = 0xcbf29ce484222325;
            for (char c : k.data) {
                h ^= static_cast<unsigned char>(c);
                h *= 0x100000001b3;
            }
            return h;
        }
    };
}

// ------------------- Node -------------------
struct Node {
    FixedKey key;
    std::string value;
    std::array<Node*, MAX_LEVEL> forward;

    // For LRU
    Node* prev_lru = nullptr;
    Node* next_lru = nullptr;

    // For ordered traversal
    Node* backward = nullptr;

    Node() {
        forward.fill(nullptr);
    }

    void init(const FixedKey& k, std::string val) {
        key = k;
        value = std::move(val);
        prev_lru = next_lru = nullptr;
        backward = nullptr;
    }

    Node* next() const { return forward[0]; }
    Node* prev() const { return backward; }
};

// ------------------- SkipList -------------------
class SkipList {
    public:
        explicit SkipList(size_t max_nodes)
            : max_nodes_(max_nodes), level_(1),
              lru_head_(nullptr), lru_tail_(nullptr) {
            node_pool_.resize(max_nodes_);
            cout << "SkipList initialized with max nodes: " << max_nodes_ << endl;
            for (auto& node : node_pool_) {
                free_list_.push_back(&node);
            }
            head_ = new Node();
            head_->init(FixedKey{}, "");
        }
    
        Node* find(const FixedKey& key) const {
            auto it = node_map_.find(key);
            return it != node_map_.end() ? it->second : nullptr;
        }
    
        Node* insert(const FixedKey& key, const std::string& value) {
            std::array<Node*, MAX_LEVEL> update;
            Node* x = head_;
    
            for (int i = level_ - 1; i >= 0; --i) {
                while (x->forward[i] && x->forward[i]->key < key) {
                    x = x->forward[i];
                }
                update[i] = x;
            }
    
            x = x->forward[0];
            if (x && x->key == key) {
                x->value = value;
                assert(lru_head_ != head_);
                return x;
            }
    
            int newLevel = randomLevel();
            if (newLevel > level_) {
                for (int i = level_; i < newLevel; ++i) {
                    update[i] = head_;
                }
                level_ = newLevel;
            }
    
            Node* new_node = allocateNode();
            new_node->init(key, value);
    
            for (int i = 0; i < newLevel; ++i) {
                new_node->forward[i] = update[i]->forward[i];
                update[i]->forward[i] = new_node;
            }
    
            new_node->backward = (update[0] == head_) ? nullptr : update[0];
            if (new_node->forward[0]) {
                new_node->forward[0]->backward = new_node;
            }
    
            node_map_[key] = new_node;
            assert(lru_head_ != head_);
            assert(new_node != head_);
            assert(check());
            return new_node;
        }
    
        Node* insert_after(Node* node, const std::string& key_str, const std::string& value) {
            if (!node) throw std::invalid_argument("insert_after: node is null");
    
            FixedKey new_key(key_str);
            if (!(node->key < new_key)) {
                throw std::invalid_argument("insert_after: key must be greater than the given node's key");
            }
    
            auto it = node_map_.find(new_key);
            if (it != node_map_.end()) {
                it->second->value = value;
                assert(lru_head_ != head_);
                return it->second;
            }
    
            std::array<Node*, MAX_LEVEL> update;
            Node* x = head_;
    
            for (int i = level_ - 1; i >= 0; --i) {
                while (x->forward[i] && x->forward[i]->key < new_key) {
                    x = x->forward[i];
                }
                update[i] = x;
            }
    
            int newLevel = randomLevel();
            if (newLevel > level_) {
                for (int i = level_; i < newLevel; ++i) {
                    update[i] = head_;
                }
                level_ = newLevel;
            }
    
            Node* new_node = allocateNode();
            new_node->init(new_key, value);
    
            for (int i = 0; i < newLevel; ++i) {
                new_node->forward[i] = update[i]->forward[i];
                update[i]->forward[i] = new_node;
            }
    
            new_node->backward = (update[0] == head_) ? nullptr : update[0];
            if (new_node->forward[0]) {
                new_node->forward[0]->backward = new_node;
            }
    
            node_map_[new_key] = new_node;
            assert(lru_head_ != head_);
            assert(new_node != head_);
            assert(check());
            return new_node;
        }
    
        void remove(Node* node) {
            if (!node) 
            {
                check();
                throw std::invalid_argument("remove: node is null");
            }
            if (node == head_) {
                check();
                throw std::invalid_argument("remove: cannot remove head node");
            }
    
            Node* x = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (x->forward[i] && x->forward[i] != node && x->forward[i]->key < node->key) {
                    x = x->forward[i];
                }
                if (x->forward[i] == node) {
                    x->forward[i] = node->forward[i];
                }
            }
    
            if (node->forward[0]) {
                node->forward[0]->backward = node->backward;
            }
    
            LRU_remove(node);
            node_map_.erase(node->key);
    
            node->value.clear();
            node->forward.fill(nullptr);
            node->backward = nullptr;
            node->prev_lru = node->next_lru = nullptr;
    
            free_list_.push_back(node);
            assert(lru_head_ != head_);
        }
    
        void LRU_append(Node* node) {
            assert(node != head_);
            if (node == lru_tail_) return;
            LRU_remove(node);
            node->prev_lru = lru_tail_;
            node->next_lru = nullptr;
            if (lru_tail_) {
                lru_tail_->next_lru = node;
            } else {
                lru_head_ = node;
            }
            lru_tail_ = node;
            assert(lru_head_ != head_);
        }
    
        void LRU_remove(Node* node) {
            if (!node) {
                throw std::invalid_argument("LRU_remove: node is null");
            }
            if (node->prev_lru) {
                node->prev_lru->next_lru = node->next_lru;
            } else if (lru_head_ == node) {
                lru_head_ = node->next_lru;
            }
            if (node->next_lru) {
                node->next_lru->prev_lru = node->prev_lru;
            } else if (lru_tail_ == node) {
                lru_tail_ = node->prev_lru;
            }
            node->prev_lru = node->next_lru = nullptr;
            assert(lru_head_ != head_);
        }
    
        Node* LRU_front() { 
            if (lru_head_ != head_)
                return lru_head_; 
            // something is wrong
            size_t lru_list_size = 0;
            for (Node* it = lru_head_; it != nullptr; it = it->next_lru) {
                lru_list_size++;
            }
            std::cerr << "LRU list size: " << lru_list_size << std::endl;
            check();
            if (lru_head_->next_lru)
            {
                lru_head_ = lru_head_->next_lru;
                lru_head_->prev_lru = nullptr;
            }
            return lru_head_;
        }
    
        Node* search(const std::string& target) const {
            FixedKey target_key(target);
            Node* x = head_;
            for (int i = level_ - 1; i >= 0; --i) {
                while (x->forward[i] && x->forward[i]->key < target_key) {
                    x = x->forward[i];
                }
            }
            Node* next = x->forward[0];
            assert(lru_head_ != head_);
            auto ret = (next && next->key == target_key) ? next : x;
            return ret;
        }
    
        void print() const {
            std::cout << "SkipList structure:\n";
            for (int lvl = level_ - 1; lvl >= 0; --lvl) {
                std::cout << "Level " << lvl << ": ";
                Node* node = head_->forward[lvl];
                while (node) {
                    std::cout << "[" << node->key.to_string() << "] ";
                    node = node->forward[lvl];
                }
                std::cout << "\n";
            }
        }
    
        void resize(size_t new_size)
        {
            if (new_size > max_nodes_)
            {
                cerr << "SkipList resizing from " << max_nodes_ << " to " << new_size << endl;
                for (size_t i = max_nodes_; i < new_size; ++i)
                {
                    node_pool_.emplace_back();
                    free_list_.push_back(&node_pool_.back());
                }
                max_nodes_ = new_size;
            }
        }

        void reset() {
            for (auto& node : node_pool_) {
                node.value.clear();
                node.forward.fill(nullptr);
                node.backward = nullptr;
                node.prev_lru = node.next_lru = nullptr;
            }
            free_list_.clear();
            for (auto& node : node_pool_) {
                free_list_.push_back(&node);
            }
            node_map_.clear();
            level_ = 1;
            lru_head_ = lru_tail_ = nullptr;
            head_->forward.fill(nullptr);
            head_->backward = nullptr;
            head_->prev_lru = head_->next_lru = nullptr;
        }

        bool check() {
            // loop check
            Node* slow_p = head_;
            Node* fast_p = head_;
            while (fast_p && fast_p->forward[0]) {
                slow_p = slow_p->forward[0];
                fast_p = fast_p->forward[0]->forward[0];
                if (slow_p == fast_p) {
                    std::cerr << "SkipList check failed: loop detected!" << std::endl;
                    std::cerr << "items in map: " << node_map_.size() << std::endl;
                    return false;
                }
            }

            size_t nodes_in_list = 0;
            for (auto it = head_->next(); it != nullptr; it = it->forward[0]) {
                nodes_in_list++;
            }
            size_t nodes_in_map = node_map_.size();
            size_t nodes_in_pool = node_pool_.size();
            size_t nodes_allocated = nodes_in_pool - free_list_.size();
            if (nodes_in_list != nodes_in_map || nodes_in_list != nodes_allocated || nodes_in_pool != max_nodes_) {
                std::cerr << "SkipList check failed: "
                            << "nodes in list: " << nodes_in_list
                            << ", nodes in map: " << nodes_in_map
                            << ", nodes allocated: " << nodes_allocated
                            << ", nodes in pool: " << nodes_in_pool
                            << ", max nodes: " << max_nodes_ << std::endl;
                return false;
            }
            return true;
        }
    const Node* head() const { return head_; }
    private:
        size_t max_nodes_;
        std::vector<Node> node_pool_;
        std::vector<Node*> free_list_;
        robin_hood::unordered_map<FixedKey, Node*> node_map_;
        Node* head_;
        int level_;
    
        Node* lru_head_;
        Node* lru_tail_;
    
        int randomLevel() const {
            int lvl = 1;
            while ((rand() & 1) && lvl < MAX_LEVEL) ++lvl;
            return lvl;
        }
    
        Node* allocateNode() {
            if (free_list_.empty()) {
                check();
                throw std::runtime_error("SkipList: out of node space!");
            }
            Node* reused = free_list_.back();
            free_list_.pop_back();
            return reused;
        }
    };
#endif