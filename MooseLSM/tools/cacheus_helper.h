#pragma once
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <optional>

// ----------------- Stubs for Python-side helpers -----------------
enum class CacheOp { INSERT, HIT };

struct Visualizinator {
    void add(const std::unordered_map<std::string, std::tuple<size_t,double,int>>&){/*noop*/}
    void addWindow(const std::unordered_map<std::string,int>&, size_t, int){/*noop*/}
    const std::vector<std::pair<size_t,double>>& get(const std::string&) const {
        static std::vector<std::pair<size_t,double>> dummy; return dummy;
    }
};

struct Pollutionator {
    explicit Pollutionator(size_t /*cache_size*/) {}
    void remove(uint64_t /*oblock*/) {}
    void incrementUniqueCount() {}
    void setUnique(uint64_t /*oblock*/) {}
    void update(size_t /*time*/) {}
};

// ----------------- Cacheus -----------------
class Cacheus {
public:
    // ------------ Internal classes ------------
    struct Cacheus_Entry {
        uint64_t oblock{};
        int      freq{1};
        size_t   time{0};
        std::optional<size_t> evicted_time;
        bool     is_demoted{false};
        bool     is_new{true};

        // Python __lt__: if freq equal, compare time desc; else freq asc
        // We’ll implement comparator outside for set ordering.
    };

    // Learning rate controller
    class Cacheus_Learning_Rate {
    public:
        explicit Cacheus_Learning_Rate(size_t period_length, double learning_rate = std::numeric_limits<double>::quiet_NaN())
        : period_len(period_length) {
            double def = std::sqrt((2.0 * std::log(2.0)) / static_cast<double>(period_length));
            if (std::isnan(learning_rate)) learning_rate_ = def;
            else learning_rate_ = learning_rate;
            learning_rate_reset = std::clamp(learning_rate_, 0.001, 1.0);
            learning_rate_curr  = learning_rate_;
            learning_rate_prev  = 0.0;
        }

        double value() const { return learning_rate_; }
        double operator*(double other) const { return learning_rate_ * other; }

        void update(size_t time) {
            if (period_len == 0) return;
            if (time % period_len != 0) { learning_rates.push_back(learning_rate_); return; }

            double hitrate_curr = round3(hitrate / static_cast<double>(period_len));
            double hitrate_diff = round3(hitrate_curr - hitrate_prev);

            double delta_LR = round3(learning_rate_curr) - round3(learning_rate_prev);
            auto [delta, unused] = updateInDeltaDirection(delta_LR, hitrate_diff);
            (void)unused; // Suppress warning

            if (delta > 0) {
                learning_rate_ = std::min(learning_rate_ + std::abs(learning_rate_ * delta_LR), 1.0);
                hitrate_nega_count = 0; hitrate_zero_count = 0;
            } else if (delta < 0) {
                learning_rate_ = std::max(learning_rate_ - std::abs(learning_rate_ * delta_LR), 0.001);
                hitrate_nega_count = 0; hitrate_zero_count = 0;
            } else if (delta == 0 && hitrate_diff <= 0) {
                if (hitrate_curr <= 0 && hitrate_diff == 0) hitrate_zero_count++;
                if (hitrate_diff < 0) { hitrate_nega_count++; hitrate_zero_count++; }
                if (hitrate_zero_count >= 10) {
                    learning_rate_ = learning_rate_reset; hitrate_zero_count = 0;
                } else if (hitrate_diff < 0) {
                    if (hitrate_nega_count >= 10) {
                        learning_rate_ = learning_rate_reset; hitrate_nega_count = 0;
                    } else {
                        updateInRandomDirection();
                    }
                }
            }

            learning_rate_prev = learning_rate_curr;
            learning_rate_curr = learning_rate_;
            hitrate_prev = hitrate_curr;
            hitrate_diff_prev = hitrate_diff;
            hitrate = 0.0;

            learning_rates.push_back(learning_rate_);
        }

        // public stats (to match Python fields)
        double hitrate{0.0};

    private:
        size_t period_len{};
        double learning_rate_{};
        double learning_rate_reset{};
        double learning_rate_curr{};
        double learning_rate_prev{};
        std::vector<double> learning_rates;

        double hitrate_prev{0.0};
        double hitrate_diff_prev{0.0};
        int    hitrate_zero_count{0};
        int    hitrate_nega_count{0};

        static double round3(double x) { return std::round(x * 1000.0) / 1000.0; }

        std::pair<int,int> updateInDeltaDirection(double learning_rate_diff, double hitrate_diff) {
            double d = learning_rate_diff * hitrate_diff;
            int delta = (d != 0.0) ? (d > 0.0 ? 1 : -1) : 0;
            int delta_HR = (delta == 0 && learning_rate_diff != 0.0) ? 0 : 1;
            return {delta, delta_HR};
        }

        void updateInRandomDirection() {
            static thread_local std::mt19937_64 rng{std::random_device{}()};
            std::uniform_int_distribution<int> coin(0,1);
            if (learning_rate_ >= 1.0) learning_rate_ = 0.9;
            else if (learning_rate_ <= 0.001) learning_rate_ = 0.005;
            else if (coin(rng) == 1) learning_rate_ = std::min(learning_rate_ * 1.25, 1.0);
            else learning_rate_ = std::max(learning_rate_ * 0.75, 0.001);
        }
    };

    // ------------ Ctor ------------
    struct Params {
        // mirrors kwargs: initial_weight, history_size, learning_rate (optional)
        double initial_weight = 0.5;
        size_t history_size_override = 0; // 0 => use default (cache_size/2)
        double learning_rate = std::numeric_limits<double>::quiet_NaN(); // NaN => auto
        int    window_size = 0; // for Visualizinator stub
    };

    Cacheus(size_t cache_size, int window_size, const Params& p)
    : time_(0),
      cache_size_(cache_size),
      history_size_(p.history_size_override ? p.history_size_override : cache_size/2),
      initial_weight_(p.initial_weight),
      learning_rate_(cache_size, std::isnan(p.learning_rate) ? std::numeric_limits<double>::quiet_NaN() : p.learning_rate),
      W_{static_cast<float>(initial_weight_), static_cast<float>(1.0 - initial_weight_)},
      visual_(),
      pollution_(cache_size) {
        // q/s split
        double hirsRatio = 0.01;
        q_limit_ = std::max<size_t>(1, static_cast<size_t>(hirsRatio * cache_size_ + 0.5));
        s_limit_ = cache_size_ - q_limit_;
        q_size_ = 0; s_size_ = 0;
        dem_count_ = 0; nor_count_ = 0;
    }

    // ------------ Public API (mirroring Python) ------------
    bool contains(uint64_t oblock) const { return s_.contains(oblock) || q_.contains(oblock); }
    bool cacheFull() const { return s_size_ + q_size_ == cache_size_; }

    std::pair<CacheOp, std::optional<uint64_t>> request(uint64_t oblock, int ts) {
        bool miss = false;
        std::optional<uint64_t> evicted;

        time_ += 1;

        // visualize (no-op)
        visual_.add({
            {"W_lru", {time_, static_cast<double>(W_[0]), ts}},
            {"W_lfu", {time_, static_cast<double>(W_[1]), ts}},
            {"q_size",{time_, static_cast<double>(q_size_), ts}},
        });

        learning_rate_.update(time_);

        if (s_.has(oblock)) {
            hitinS(oblock);
        } else if (q_.has(oblock)) {
            hitinQ(oblock);
        } else if (lru_hist_.has(oblock)) {
            miss = true;
            evicted = hitinLRUHist(oblock);
        } else if (lfu_hist_.has(oblock)) {
            miss = true;
            evicted = hitinLFUHist(oblock);
        } else {
            miss = true;
            evicted = missPath(oblock);
        }

        // windowed visualize
        visual_.addWindow({{"hit-rate", miss ? 0 : 1}}, time_, ts);

        // learning rate accounting
        if (!miss) learning_rate_.hitrate += 1.0;

        // pollution (no-op stubs)
        if (miss) pollution_.incrementUniqueCount();
        pollution_.setUnique(oblock);
        if (time_ % cache_size_ == 0) pollution_.update(time_);

        return {miss ? CacheOp::INSERT : CacheOp::HIT, evicted};
    }

    // helper for tests
    size_t getQsize() const { return q_size_; }

private:
    // ------------ OrderedMap (DequeDict equivalent) ------------
    class OrderedMap {
    public:
        bool contains(uint64_t k) const { return map_.count(k) != 0; }
        bool has(uint64_t k) const { return contains(k); }

        Cacheus_Entry& operator[](uint64_t k) {
            auto it = map_.find(k);
            if (it == map_.end()) {
                order_.push_back(k);
                auto iit = std::prev(order_.end());
                auto [ins, _] = map_.emplace(k, Node{Cacheus_Entry{.oblock=k}, iit});
                return ins->second.ent;
            } else {
                // touch to MRU (back)
                touch(it);
                return it->second.ent;
            }
        }

        const Cacheus_Entry& at(uint64_t k) const { return map_.at(k).ent; }
        Cacheus_Entry& at(uint64_t k) { return map_.at(k).ent; }

        void set(uint64_t k, const Cacheus_Entry& e) {
            auto it = map_.find(k);
            if (it == map_.end()) {
                order_.push_back(k);
                auto iit = std::prev(order_.end());
                map_.emplace(k, Node{e, iit});
            } else {
                it->second.ent = e;
                touch(it);
            }
        }

        bool erase(uint64_t k) {
            auto it = map_.find(k);
            if (it == map_.end()) return false;
            order_.erase(it->second.pos);
            map_.erase(it);
            return true;
        }

        // LRU = front
        Cacheus_Entry first() const {
            assert(!order_.empty());
            return map_.at(order_.front()).ent;
        }

        // popFirst(): remove and return LRU
        Cacheus_Entry popFirst() {
            assert(!order_.empty());
            auto k = order_.front();
            auto e = map_.at(k).ent;
            order_.pop_front();
            map_.erase(k);
            return e;
        }

        size_t size() const { return map_.size(); }
        bool empty() const { return map_.empty(); }

        // direct access to keys (needed by lfu sync)
        bool get(uint64_t k, Cacheus_Entry& out) const {
            auto it = map_.find(k); if (it == map_.end()) return false;
            out = it->second.ent; return true;
        }

    private:
        struct Node {
            Cacheus_Entry ent;
            std::list<uint64_t>::iterator pos;
        };
        std::list<uint64_t> order_;
        std::unordered_map<uint64_t, Node> map_;

        void touch(typename std::unordered_map<uint64_t,Node>::iterator it) {
            order_.erase(it->second.pos);
            order_.push_back(it->first);
            it->second.pos = std::prev(order_.end());
        }
    };

    // ------------ LFU index (HeapDict equivalent) ------------
    // We keep a std::set ordered by (freq asc, time desc, oblock asc) for stable min()
    struct LFUKey {
        int    freq;
        size_t time_desc; // store as (max - time) negative? easier: compare reversed
        uint64_t oblock;

        bool operator<(const LFUKey& o) const {
            if (freq != o.freq) return freq < o.freq;
            if (time_desc != o.time_desc) return time_desc < o.time_desc; // time desc
            return oblock < o.oblock;
        }
    };

    class LFUIndex {
    public:
        // insert/update full entry
        void set(const Cacheus_Entry& e) {
            auto it = pos_.find(e.oblock);
            if (it != pos_.end()) {
                set_.erase(it->second);
            }
            LFUKey k{e.freq, timeDesc(e.time), e.oblock};
            auto iter = set_.insert(k).first;
            pos_[e.oblock] = iter;
        }
        bool erase(uint64_t oblock) {
            auto p = pos_.find(oblock);
            if (p == pos_.end()) return false;
            set_.erase(p->second);
            pos_.erase(p);
            return true;
        }
        // Return pointer-like view (we only need oblock/freq/time)
        Cacheus_Entry min(const OrderedMap& s, const OrderedMap& q) const {
            assert(!set_.empty());
            auto k = *set_.begin();
            Cacheus_Entry e{};
            // find the actual entry in s or q
            if (!s.get(k.oblock, e)) {
                bool ok = q.get(k.oblock, e);
                (void)ok;
            }
            return e;
        }
        bool empty() const { return set_.empty(); }

    private:
        static size_t timeDesc(size_t t) { return std::numeric_limits<size_t>::max() - t; }

        std::set<LFUKey> set_;
        std::unordered_map<uint64_t, std::set<LFUKey>::iterator> pos_;
    };

    // ------------ Core methods ------------
    void hitinS(uint64_t oblock) {
        auto x = s_.at(oblock);
        x.time = time_;
        x.freq += 1;
        s_.set(oblock, x);
        lfu_.set(x);
    }

    void hitinQ(uint64_t oblock) {
        auto x = q_.at(oblock);
        x.time = time_;
        x.freq += 1;
        lfu_.set(x);

        if (x.is_demoted) {
            adjustSize(true);
            x.is_demoted = false;
            dem_count_ -= 1;
        }
        q_.erase(x.oblock);
        q_size_ -= 1;

        if (s_size_ >= s_limit_) {
            auto y = s_.popFirst();
            y.is_demoted = true;
            dem_count_ += 1;
            s_size_ -= 1;
            q_.set(y.oblock, y);
            q_size_ += 1;
        }

        s_.set(x.oblock, x);
        s_size_ += 1;
    }

    void addToS(uint64_t oblock, int freq, bool isNew = true) {
        Cacheus_Entry x{.oblock=oblock, .freq=freq, .time=time_, .evicted_time=std::nullopt, .is_demoted=false, .is_new=isNew};
        s_.set(oblock, x);
        lfu_.set(x);
        s_size_ += 1;
    }

    void addToQ(uint64_t oblock, int freq, bool isNew = true) {
        Cacheus_Entry x{.oblock=oblock, .freq=freq, .time=time_, .evicted_time=std::nullopt, .is_demoted=false, .is_new=isNew};
        q_.set(oblock, x);
        lfu_.set(x);
        q_size_ += 1;
    }

    void addToHistory(const Cacheus_Entry& x_in, int policy) {
        if (policy == -1) return;
        auto x = x_in;
        OrderedMap* hist = nullptr;
        if (policy == 0) {
            hist = &lru_hist_;
            if (x.is_new) nor_count_ += 1;
        } else if (policy == 1) {
            hist = &lfu_hist_;
        } else return;

        if (hist->size() == history_size_) {
            auto ev = getLRU(*hist);
            hist->erase(ev.oblock);
            if (hist == &lru_hist_ && ev.is_new) {
                nor_count_ -= 1;
            }
        }
        hist->set(x.oblock, x);
    }

    Cacheus_Entry getLRU(const OrderedMap& d) const { return d.first(); }

    Cacheus_Entry getHeapMin() const {
        return lfu_.min(s_, q_);
    }

    int getChoice() {
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        std::bernoulli_distribution bern(W_[0]);
        return bern(rng) ? 0 : 1;
    }

    std::pair<uint64_t,int> evictOne() {
        auto lru = getLRU(q_);
        auto lfu = getHeapMin();

        Cacheus_Entry evicted = lru;
        int policy = getChoice();

        if (lru.oblock == lfu.oblock) {
            evicted = lru; policy = -1;
        } else if (policy == 0) {
            evicted = lru;
            q_.erase(evicted.oblock);
            q_size_ -= 1;
        } else if (policy == 1) {
            evicted = lfu;
            if (s_.contains(evicted.oblock)) {
                s_.erase(evicted.oblock);
                s_size_ -= 1;
            } else if (q_.contains(evicted.oblock)) {
                q_.erase(evicted.oblock);
                q_size_ -= 1;
            }
        }

        if (evicted.is_demoted) {
            dem_count_ -= 1;
            evicted.is_demoted = false;
        }

        if (policy == -1) {
            q_.erase(evicted.oblock);
            q_size_ -= 1;
        }

        lfu_.erase(evicted.oblock);
        evicted.evicted_time = time_;
        pollution_.remove(evicted.oblock);

        addToHistory(evicted, policy);

        return {evicted.oblock, policy};
    }

    void adjustWeights(double rewardLRU, double rewardLFU) {
        double lr = learning_rate_.value();
        double e0 = std::exp(lr * rewardLRU);
        double e1 = std::exp(lr * rewardLFU);
        double n0 = static_cast<double>(W_[0]) * e0;
        double n1 = static_cast<double>(W_[1]) * e1;
        double sum = n0 + n1;
        float w0 = static_cast<float>(n0 / sum);
        float w1 = static_cast<float>(n1 / sum);
        if (w0 >= 0.99f) { w0 = 0.99f; w1 = 0.01f; }
        else if (w1 >= 0.99f) { w0 = 0.01f; w1 = 0.99f; }
        W_[0] = w0; W_[1] = w1;
    }

    void adjustSize(bool hit_in_Q) {
        if (hit_in_Q) {
            size_t dem_count = std::max<size_t>(1, dem_count_);
            s_limit_ = std::min(cache_size_ - 1,
                        s_limit_ + std::max<size_t>(1, static_cast<size_t>( (nor_count_ / static_cast<double>(dem_count)) + 0.5 )));
            q_limit_ = cache_size_ - s_limit_;
        } else {
            size_t nor_count = std::max<size_t>(1, nor_count_);
            q_limit_ = std::min(cache_size_ - 1,
                        q_limit_ + std::max<size_t>(1, static_cast<size_t>( (dem_count_ / static_cast<double>(nor_count)) + 0.5 )));
            s_limit_ = cache_size_ - q_limit_;
        }
    }

    std::optional<uint64_t> hitinLRUHist(uint64_t oblock) {
        std::optional<uint64_t> ev;
        auto entry = lru_hist_.at(oblock);
        int freq = entry.freq + 1; (void)freq; // Note: Python increments local, then uses entry.freq
        lru_hist_.erase(oblock);
        if (entry.is_new) {
            nor_count_ -= 1;
            entry.is_new = false;
            adjustSize(false);
        }
        adjustWeights(-1, 0);

        if (s_size_ + q_size_ >= cache_size_) {
            auto [e, policy] = evictOne();
            ev = e; (void)policy;
        }
        addToS(entry.oblock, entry.freq, /*isNew=*/false);
        limitStack();
        return ev;
    }

    std::optional<uint64_t> hitinLFUHist(uint64_t oblock) {
        std::optional<uint64_t> ev;
        auto entry = lfu_hist_.at(oblock);
        int freq = entry.freq + 1; (void)freq;
        lfu_hist_.erase(oblock);
        adjustWeights(0, -1);

        if (s_size_ + q_size_ >= cache_size_) {
            auto [e, policy] = evictOne();
            ev = e; (void)policy;
        }
        addToS(entry.oblock, entry.freq, /*isNew=*/false);
        limitStack();
        return ev;
    }

    void limitStack() {
        while (s_size_ >= s_limit_) {
            auto demoted = s_.popFirst();
            s_size_ -= 1;
            demoted.is_demoted = true;
            dem_count_ += 1;
            q_.set(demoted.oblock, demoted);
            q_size_ += 1;
        }
    }

    std::optional<uint64_t> missPath(uint64_t oblock) {
        std::optional<uint64_t> ev;
        int freq = 1;

        if (s_size_ < s_limit_ && q_size_ == 0) {
            addToS(oblock, freq, /*isNew=*/false);
        } else if (s_size_ + q_size_ < cache_size_ && q_size_ < q_limit_) {
            addToQ(oblock, freq, /*isNew=*/false);
        } else {
            if (s_size_ + q_size_ >= cache_size_) {
                auto [e, policy] = evictOne();
                ev = e; (void)policy;
            }
            addToQ(oblock, freq, /*isNew=*/true);
            limitStack();
        }
        return ev;
    }

private:
    // state
    size_t time_{0};

    size_t cache_size_{0};
    size_t history_size_{0};

    // two stacks
    OrderedMap s_; // MRU region
    OrderedMap q_; // LRU region

    // lfu heap/index
    LFUIndex lfu_;

    // histories
    OrderedMap lru_hist_;
    OrderedMap lfu_hist_;

    // weights
    double initial_weight_{0.5};
    Cacheus_Learning_Rate learning_rate_;
    float W_[2]{0.5f, 0.5f};

    // sizes and counters
    size_t q_limit_{0};
    size_t s_limit_{0};
    size_t q_size_{0};
    size_t s_size_{0};
    size_t dem_count_{0};
    size_t nor_count_{0};
    std::vector<size_t> q_sizes_; // kept for parity; unused

    // viz & pollution (stubs)
    Visualizinator visual_;
    Pollutionator  pollution_;
};
