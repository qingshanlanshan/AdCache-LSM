#include "EvictionPolicy.h"
#include "Utils.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <list>
#include <queue>
#include <optional>
#include <cmath>
#include <random>
#include <limits>
#include <memory>
#include <cassert>

// A faithful C++ port of the Python `Cacheus` policy, including:
// - Two-stack cache (S = MRU stack, Q = LRU stack)
// - LFU structure (min-heap with lazy-stale filtering)
// - Histories (lru_hist / lfu_hist) with bounded size
// - Adaptive multiplicative-weights learning rate controller
// - Dynamic S/Q partitioning (adjustSize, limitStack)
// - Pollution and Visualization placeholders (lightweight, compile-ready)
//
// Notes
// -----
// * This class assumes `K` is hashable and equality-comparable (as declared in Utils.h).
// * Memory is managed with shared_ptr<Entry>, so entries can move across containers safely.
// * The LFU min-heap uses a ticket (version) to lazily discard stale heap nodes.
// * Visualization and Pollution tracking are implemented as lightweight helpers to preserve calls.

class Cacheus : public EvictionPolicy {
public:
    using Time = std::size_t;

    struct Entry {
        K key;
        int freq;
        Time time;                 // last touch time (monotonic)
        std::optional<Time> evicted_time;
        bool is_demoted;
        bool is_new;
        std::uint64_t ticket;      // version for LFU heap

        Entry(const K& k, int f, Time t, bool isNew)
            : key(k), freq(f), time(t), evicted_time(std::nullopt),
              is_demoted(false), is_new(isNew), ticket(0) {}
    };

    // --------------------- Helpers: DequeDict ---------------------
    // (Ordered dict supporting O(1) contains/erase, LRU popFirst, MRU push/set)
    class DequeDict {
    public:
        using It = std::list<K>::iterator;

        bool contains(const K& k) const { return map_.count(k) != 0; }
        std::size_t size() const { return map_.size(); }
        bool empty() const { return map_.empty(); }

        // Get entry (shared_ptr)
        std::shared_ptr<Entry>& at(const K& k) { return map_.at(k).second; }
        const std::shared_ptr<Entry>& at(const K& k) const { return map_.at(k).second; }

        // Insert or move to MRU (back); set pointer
        void set(const K& k, const std::shared_ptr<Entry>& e) {
            auto it = map_.find(k);
            if (it != map_.end()) {
                order_.erase(it->second.first);
                order_.push_back(k);
                it->second = { std::prev(order_.end()), e };
            } else {
                order_.push_back(k);
                map_.emplace(k, std::make_pair(std::prev(order_.end()), e));
            }
        }

        // Erase by key; returns the removed entry (or nullptr if absent)
        std::shared_ptr<Entry> erase(const K& k) {
            auto it = map_.find(k);
            if (it == map_.end()) return nullptr;
            order_.erase(it->second.first);
            auto ptr = it->second.second;
            map_.erase(it);
            return ptr;
        }

        // LRU (front) without removal
        std::shared_ptr<Entry> first() const {
            if (order_.empty()) return nullptr;
            const K& k = order_.front();
            return map_.at(k).second;
        }

        // Pop LRU (front)
        std::shared_ptr<Entry> popFirst() {
            if (order_.empty()) return nullptr;
            const K k = order_.front();
            order_.pop_front();
            auto it = map_.find(k);
            assert(it != map_.end());
            auto ptr = it->second.second;
            map_.erase(it);
            return ptr;
        }

        // Access or insert default (used to mimic Python's d[k])
        std::shared_ptr<Entry>& operator[](const K& k) { return map_.at(k).second; }

    private:
        std::list<K> order_;
        std::unordered_map<K, std::pair<It, std::shared_ptr<Entry>>> map_;
    };

    // --------------------- Helpers: LFU Min-Heap ---------------------
    struct LFUNode {
        int freq;
        // For LRU tie-breaker prefer newer: Python uses `__lt__` with time >
        // We want min by freq, and for equal freq choose smaller priority if time newer.
        // We'll encode as: key for min-heap = (freq, -time)
        std::int64_t neg_time;
        std::uint64_t ticket;
        K key;
    };

    struct LFUNodeLess { // for std::priority_queue (max-heap), invert to get min-heap
        bool operator()(const LFUNode& a, const LFUNode& b) const {
            if (a.freq != b.freq) return a.freq > b.freq;           // smaller freq first
            if (a.neg_time != b.neg_time) return a.neg_time < b.neg_time; // more recent first (more negative is smaller)
            return a.ticket > b.ticket; // smaller ticket first
        }
    };

    // --------------------- Helpers: Visualization & Pollution ---------------------
    struct Visualizinator {
        struct Triple { Time t; double v; double ts; };
        std::unordered_map<std::string, std::vector<Triple>> series;
        std::unordered_map<std::string, std::vector<Triple>> window_series;
        void add(const std::unordered_map<std::string, std::tuple<Time,double,double>>& items) {
            for (auto& [name, tup] : items) {
                series[name].push_back(Triple{ std::get<0>(tup), std::get<1>(tup), std::get<2>(tup) });
            }
        }
        void addWindow(const std::unordered_map<std::string, double>& items, Time t, double ts) {
            for (auto& [name, v] : items) window_series[name].push_back(Triple{ t, v, ts });
        }
        std::vector<std::pair<Time,double>> get(const std::string& name) const {
            std::vector<std::pair<Time,double>> out;
            auto it = series.find(name);
            if (it == series.end()) return out;
            out.reserve(it->second.size());
            for (auto& tr : it->second) out.emplace_back(tr.t, tr.v);
            return out;
        }
    };

    struct Pollutionator {
        explicit Pollutionator(std::size_t /*cache_size*/) {}
        void remove(const K& k) { unique_.erase(k); }
        void incrementUniqueCount() { ++unique_in_period_; }
        void setUnique(const K& k) { unique_.insert(k); }
        void update(Time /*t*/) { unique_in_period_ = 0; }
    private:
        std::unordered_set<K> unique_;
        std::size_t unique_in_period_{0};
    };

    // --------------------- Adaptive Learning Rate ---------------------
    class Cacheus_Learning_Rate {
    public:
        explicit Cacheus_Learning_Rate(std::size_t period_length, double initial_lr = std::numeric_limits<double>::quiet_NaN())
            : period_len_(period_length) {
            double base = std::sqrt((2.0 * std::log(2.0)) / std::max<std::size_t>(1, period_length));
            learning_rate_ = std::isnan(initial_lr) ? base : initial_lr;
            learning_rate_reset_ = std::clamp(learning_rate_, 0.001, 1.0);
            learning_rate_curr_ = learning_rate_;
            learning_rate_prev_ = 0.0;
        }

        double value() const { return learning_rate_; }

        void note_hit() { ++hitrate_; }

        void update(Time time) {
            if (period_len_ == 0) return;
            if (time % period_len_ == 0) {
                double hitrate_curr = round3(hitrate_ / static_cast<double>(period_len_));
                double hitrate_diff = round3(hitrate_curr - hitrate_prev_);

                double delta_LR = round3(learning_rate_curr_) - round3(learning_rate_prev_);
                int delta, delta_HR;
                std::tie(delta, delta_HR) = updateInDeltaDirection(delta_LR, hitrate_diff);

                if (delta > 0) {
                    learning_rate_ = std::min(learning_rate_ + std::fabs(learning_rate_ * delta_LR), 1.0);
                    hitrate_nega_count_ = 0; hitrate_zero_count_ = 0;
                } else if (delta < 0) {
                    learning_rate_ = std::max(learning_rate_ - std::fabs(learning_rate_ * delta_LR), 0.001);
                    hitrate_nega_count_ = 0; hitrate_zero_count_ = 0;
                } else if (delta == 0 && hitrate_diff <= 0) {
                    if (hitrate_curr <= 0 && hitrate_diff == 0) ++hitrate_zero_count_;
                    if (hitrate_diff < 0) { ++hitrate_nega_count_; ++hitrate_zero_count_; }
                    if (hitrate_zero_count_ >= 10) {
                        learning_rate_ = learning_rate_reset_; hitrate_zero_count_ = 0;
                    } else if (hitrate_diff < 0) {
                        if (hitrate_nega_count_ >= 10) {
                            learning_rate_ = learning_rate_reset_; hitrate_nega_count_ = 0;
                        } else {
                            updateInRandomDirection();
                        }
                    }
                }

                learning_rate_prev_ = learning_rate_curr_;
                learning_rate_curr_ = learning_rate_;
                hitrate_prev_ = hitrate_curr;
                hitrate_diff_prev_ = hitrate_diff;
                hitrate_ = 0;
            }
            learning_rates_.push_back(learning_rate_);
        }

    private:
        static double round3(double x) { return std::round(x * 1000.0) / 1000.0; }

        std::pair<int,int> updateInDeltaDirection(double lr_diff, double hr_diff) {
            double prod = lr_diff * hr_diff;
            int delta = (prod != 0.0) ? (prod > 0 ? 1 : -1) : 0;
            int delta_HR = (delta == 0 && lr_diff != 0.0) ? 0 : 1;
            (void)delta_HR; // kept to mirror Python signature; not used elsewhere
            return {delta, delta_HR};
        }

        void updateInRandomDirection() {
            if (learning_rate_ >= 1.0) learning_rate_ = 0.9;
            else if (learning_rate_ <= 0.001) learning_rate_ = 0.005;
            else if (coin_flip_()) learning_rate_ = std::min(learning_rate_ * 1.25, 1.0);
            else learning_rate_ = std::max(learning_rate_ * 0.75, 0.001);
        }

        static bool coin_flip_() {
            static thread_local std::mt19937_64 gen{std::random_device{}()};
            static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
            return dist(gen) < 0.5;
        }

        // State
        double learning_rate_{};
        double learning_rate_reset_{};
        double learning_rate_curr_{};
        double learning_rate_prev_{};
        std::vector<double> learning_rates_{};
        std::size_t period_len_{};

        double hitrate_ = 0.0;
        double hitrate_prev_ = 0.0;
        double hitrate_diff_prev_ = 0.0;
        int hitrate_zero_count_ = 0;
        int hitrate_nega_count_ = 0;
    };

    // --------------------- Constructor ---------------------
    Cacheus(std::size_t cache_capacity, std::size_t window_size = 128, double initial_weight = 0.5)
        : EvictionPolicy(cache_capacity),
          time_(0),
          cache_size_(cache_capacity),
          history_size_(cache_capacity / 2),
          learning_rate_(cache_capacity),
          visual_(),
          pollution_(cache_capacity) {
        if (cache_size_ == 0) cache_size_ = 1; // avoid division by zero
        // Initial W
        W_[0] = static_cast<float>(initial_weight);
        W_[1] = 1.0f - W_[0];
        // Partition
        double hirsRatio = 0.01; // as Python
        q_limit_ = std::max<std::size_t>(1, static_cast<std::size_t>(hirsRatio * cache_size_ + 0.5));
        s_limit_ = cache_size_ - q_limit_;
    }

    // --------------------- EvictionPolicy interface ---------------------
    std::optional<K> request(K key) override {
        // In Python, `ts` is passed in — here we just reuse time_ as ts for logging.
        double ts = static_cast<double>(time_);
        time_ += 1;

        visual_.add({
            {"W_lru", std::make_tuple(time_, static_cast<double>(W_[0]), ts)},
            {"W_lfu", std::make_tuple(time_, static_cast<double>(W_[1]), ts)},
            {"q_size", std::make_tuple(time_, static_cast<double>(q_.size()), ts)}
        });

        learning_rate_.update(time_);

        bool miss = false;
        std::optional<K> evicted_key = std::nullopt;

        if (s_.contains(key)) {
            hitinS_(key);
        } else if (q_.contains(key)) {
            hitinQ_(key);
        } else if (lru_hist_.contains(key)) {
            miss = true;
            auto ek = hitinLRUHist_(key);
            if (ek) evicted_key = ek;
        } else if (lfu_hist_.contains(key)) {
            miss = true;
            auto ek = hitinLFUHist_(key);
            if (ek) evicted_key = ek;
        } else {
            miss = true;
            auto ek = miss_(key);
            if (ek) evicted_key = ek;
        }

        // Windowed hit-rate
        visual_.addWindow({ {"hit-rate", miss ? 0.0 : 1.0} }, time_, ts);
        if (!miss) learning_rate_.note_hit();

        // Pollution tracking
        if (miss) pollution_.incrementUniqueCount();
        pollution_.setUnique(key);
        if (time_ % cache_size_ == 0) pollution_.update(time_);

        return evicted_key; // None if no eviction occurred on this request
    }

    std::vector<K> set_capacity(std::size_t capacity) override {
        std::vector<K> evicted;
        cache_size_ = std::max<std::size_t>(1, capacity);
        // Recompute limits keeping current ratio
        q_limit_ = std::max<std::size_t>(1, static_cast<std::size_t>(0.01 * cache_size_ + 0.5));
        s_limit_ = cache_size_ - q_limit_;
        // Evict until total <= capacity
        while (s_.size() + q_.size() > cache_size_) {
            auto e = evict_();
            if (e) evicted.push_back(*e);
            else break; // shouldn't happen, but avoid infinite loop
        }
        // Adjust history_size to mirror Python behavior (cache_size // 2)
        history_size_ = cache_size_ / 2;
        return evicted;
    }

    bool warmup_done() override {
        return (s_.size() + q_.size()) >= cache_size_;
    }

    // Optional: expose q_size timeline similar to Python's getQsize()
    std::vector<double> getQsizeSeries() const {
        std::vector<double> ys;
        auto pairs = visual_.get("q_size");
        ys.reserve(pairs.size());
        for (auto& p : pairs) ys.push_back(p.second);
        return ys;
    }

private:
    // --------------------- Core state ---------------------
    Time time_;
    std::size_t cache_size_;

    DequeDict s_; // MRU stack (S)
    DequeDict q_; // LRU stack (Q)

    DequeDict lru_hist_;
    DequeDict lfu_hist_;

    std::priority_queue<LFUNode, std::vector<LFUNode>, LFUNodeLess> lfu_heap_;

    std::size_t history_size_;
    float W_[2]; // [W_lru, W_lfu]

    std::size_t q_limit_{};
    std::size_t s_limit_{};
    std::size_t dem_count_ = 0;
    std::size_t nor_count_ = 0;

    Cacheus_Learning_Rate learning_rate_;
    Visualizinator visual_;
    Pollutionator pollution_;

    // --------------------- Utility ---------------------
    static bool coin_flip_() {
        static thread_local std::mt19937_64 gen{std::random_device{}()};
        static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(gen) < 0.5;
    }

    void push_lfu_snapshot_(const std::shared_ptr<Entry>& e) {
        // Snapshot with incremented ticket
        ++(e->ticket);
        lfu_heap_.push(LFUNode{ e->freq, -static_cast<std::int64_t>(e->time), e->ticket, e->key });
    }

    std::shared_ptr<Entry> getHeapMin_() {
        while (!lfu_heap_.empty()) {
            LFUNode top = lfu_heap_.top();
            // Ensure the key is still in S or Q and the ticket matches
            std::shared_ptr<Entry> cur;
            if (s_.contains(top.key)) cur = s_.at(top.key);
            else if (q_.contains(top.key)) cur = q_.at(top.key);
            if (!cur) { lfu_heap_.pop(); continue; }
            if (cur->ticket != top.ticket) { lfu_heap_.pop(); continue; }
            // Valid
            return cur;
        }
        return nullptr;
    }

    std::shared_ptr<Entry> getLRU_(const DequeDict& d) const { return d.first(); }

    int getChoice_() const {
        // 0 -> choose LRU; 1 -> choose LFU
        static thread_local std::mt19937_64 gen{std::random_device{}()};
        static thread_local std::uniform_real_distribution<double> dist(0.0, 1.0);
        return (dist(gen) < W_[0]) ? 0 : 1;
    }

    void adjustWeights_(double rewardLRU, double rewardLFU) {
        double lr = learning_rate_.value();
        double w0 = W_[0] * std::exp(lr * rewardLRU);
        double w1 = W_[1] * std::exp(lr * rewardLFU);
        double s = w0 + w1;
        if (s <= 0) { W_[0] = 0.5f; W_[1] = 0.5f; }
        else { W_[0] = static_cast<float>(w0 / s); W_[1] = static_cast<float>(w1 / s); }
        if (W_[0] >= 0.99f) { W_[0] = 0.99f; W_[1] = 0.01f; }
        else if (W_[1] >= 0.99f) { W_[0] = 0.01f; W_[1] = 0.99f; }
    }

    void adjustSize_(bool hit_in_Q) {
        if (hit_in_Q) {
            std::size_t dem_count = std::max<std::size_t>(1, dem_count_);
            std::size_t inc = std::max<std::size_t>(1, static_cast<std::size_t>( (static_cast<double>(nor_count_) / dem_count) + 0.5 ));
            s_limit_ = std::min<std::size_t>(cache_size_ - 1, s_limit_ + inc);
            q_limit_ = cache_size_ - s_limit_;
        } else {
            std::size_t nor_count = std::max<std::size_t>(1, nor_count_);
            std::size_t inc = std::max<std::size_t>(1, static_cast<std::size_t>( (static_cast<double>(dem_count_) / nor_count) + 0.5 ));
            q_limit_ = std::min<std::size_t>(cache_size_ - 1, q_limit_ + inc);
            s_limit_ = cache_size_ - q_limit_;
        }
    }

    void limitStack_() {
        while (s_.size() >= s_limit_) {
            auto demoted = s_.popFirst();
            if (!demoted) break;
            demoted->is_demoted = true;
            ++dem_count_;
            q_.set(demoted->key, demoted);
        }
    }

    // --------------------- Operations mirroring Python ---------------------
    void hitinS_(const K& key) {
        auto x = s_.at(key);
        x->time = time_;
        s_.set(key, x);           // move to MRU
        x->freq += 1;
        push_lfu_snapshot_(x);
    }

    void hitinQ_(const K& key) {
        auto x = q_.at(key);
        x->time = time_;
        x->freq += 1;
        push_lfu_snapshot_(x);

        if (x->is_demoted) {
            adjustSize_(true);
            x->is_demoted = false;
            if (dem_count_ > 0) --dem_count_;
        }
        q_.erase(x->key);

        if (s_.size() >= s_limit_) {
            auto y = s_.popFirst();
            if (y) {
                y->is_demoted = true;
                ++dem_count_;
                q_.set(y->key, y);
            }
        }
        s_.set(x->key, x);
    }

    void addToS_(const K& key, int freq, bool isNew) {
        auto e = std::make_shared<Entry>(key, freq, time_, isNew);
        s_.set(key, e);
        push_lfu_snapshot_(e);
    }

    void addToQ_(const K& key, int freq, bool isNew) {
        auto e = std::make_shared<Entry>(key, freq, time_, isNew);
        q_.set(key, e);
        push_lfu_snapshot_(e);
    }

    void addToHistory_(const std::shared_ptr<Entry>& x, int policy) {
        // policy: 0 -> LRU history, 1 -> LFU history, -1 -> none
        if (policy == -1) return;
        DequeDict* hist = nullptr;
        if (policy == 0) {
            hist = &lru_hist_;
            if (x->is_new) ++nor_count_;
        } else if (policy == 1) {
            hist = &lfu_hist_;
        } else return;

        if (hist->size() == history_size_) {
            auto evicted = getLRU_(*hist);
            if (evicted) {
                hist->erase(evicted->key);
                if (hist == &lru_hist_ && evicted->is_new) {
                    if (nor_count_ > 0) --nor_count_;
                }
            }
        }
        hist->set(x->key, x);
    }

    std::shared_ptr<Entry> getLRU_(DequeDict& d) { return d.first(); }

    std::optional<K> evict_() {
        auto lru = getLRU_(q_);
        auto lfu = getHeapMin_();

        if (!lru && !lfu) return std::nullopt; // nothing to evict
        std::shared_ptr<Entry> evicted;
        int policy = getChoice_(); // 0 LRU, 1 LFU

        bool same = false;
        if (lru && lfu) same = (lru->key == lfu->key);

        if (same) {
            evicted = lru; // policy = -1 (special)
        } else if (policy == 0) {
            evicted = lru ? lru : lfu; // if no lru, fall back to lfu
            if (evicted && q_.contains(evicted->key)) q_.erase(evicted->key);
        } else { // policy == 1
            evicted = lfu ? lfu : lru;
            if (evicted) {
                if (s_.contains(evicted->key)) s_.erase(evicted->key);
                else if (q_.contains(evicted->key)) q_.erase(evicted->key);
            }
        }

        if (!evicted) return std::nullopt;

        if (evicted->is_demoted) { if (dem_count_ > 0) --dem_count_; evicted->is_demoted = false; }

        if (same) {
            if (q_.contains(evicted->key)) q_.erase(evicted->key);
        }

        evicted->evicted_time = time_;
        pollution_.remove(evicted->key);

        addToHistory_(evicted, same ? -1 : policy);
        return evicted->key;
    }

    std::optional<K> hitinLRUHist_(const K& key) {
        std::optional<K> evicted_key = std::nullopt;
        auto entry = lru_hist_.erase(key); // remove from hist
        if (!entry) return std::nullopt;
        int freq_plus_one = entry->freq + 1; // Python computes but doesn't use
        (void)freq_plus_one;
        if (entry->is_new) {
            if (nor_count_ > 0) --nor_count_;
            entry->is_new = false;
            adjustSize_(false);
        }
        adjustWeights_(-1.0, 0.0);

        if (s_.size() + q_.size() >= cache_size_) {
            auto e = evict_();
            if (e) evicted_key = e;
        }
        addToS_(entry->key, entry->freq, false);
        limitStack_();
        return evicted_key;
    }

    std::optional<K> hitinLFUHist_(const K& key) {
        std::optional<K> evicted_key = std::nullopt;
        auto entry = lfu_hist_.erase(key);
        if (!entry) return std::nullopt;
        int freq_plus_one = entry->freq + 1; (void)freq_plus_one;
        adjustWeights_(0.0, -1.0);

        if (s_.size() + q_.size() >= cache_size_) {
            auto e = evict_();
            if (e) evicted_key = e;
        }
        addToS_(entry->key, entry->freq, false);
        limitStack_();
        return evicted_key;
    }

    std::optional<K> miss_(const K& key) {
        std::optional<K> evicted_key = std::nullopt;
        int freq = 1;
        if (s_.size() < s_limit_ && q_.size() == 0) {
            addToS_(key, freq, false);
        } else if (s_.size() + q_.size() < cache_size_ && q_.size() < q_limit_) {
            addToQ_(key, freq, false);
        } else {
            if (s_.size() + q_.size() >= cache_size_) {
                auto e = evict_();
                if (e) evicted_key = e;
            }
            addToQ_(key, freq, true);
            limitStack_();
        }
        return evicted_key;
    }
};
