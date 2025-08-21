#pragma once

#include "Utils.h"

class EvictionPolicy {
    public:
        EvictionPolicy(size_t) {};
        virtual ~EvictionPolicy() = default;

        virtual std::optional<K> request(K key) = 0;
        // AdCache specific
        virtual std::vector<K> set_capacity(size_t capacity) {
            (void)capacity;
            return {};
        }
        virtual bool warmup_done() const { return true; }
        virtual std::optional<K> peek_victim() const { return std::nullopt; }
};