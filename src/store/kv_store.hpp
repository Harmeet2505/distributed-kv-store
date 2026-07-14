#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include "persistence/wal.hpp"

class KVStore{
    public:
        explicit KVStore(const std:: string &walpath);

        void set(const std:: string &key , const std:: string & value);
        std:: string get(const std:: string &key);
        bool del(const std:: string &key);

    private:
        std::unordered_map<std:: string ,std:: string> data_;
        mutable std:: shared_mutex mutex_;
        WAL wal_;
};

