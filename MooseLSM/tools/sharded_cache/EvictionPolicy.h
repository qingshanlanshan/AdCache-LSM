#pragma once

#include "Utils.h"

class EvictionPolicy {
    public:
        EvictionPolicy(size_t) {};
        virtual ~EvictionPolicy() = default;

        virtual std::optional<K> request(K key) = 0;
        virtual std::vector<K> set_capacity(size_t capacity) = 0;
        virtual bool warmup_done() = 0;
};