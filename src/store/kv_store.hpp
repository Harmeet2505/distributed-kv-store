#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include "persistence/wal.hpp"
#include "persistence/snapshot.hpp"

class KVStore{
    public:
        explicit KVStore(const std:: string &walpath , const std:: string &snapshotPath);

        void set(const std:: string &key , const std:: string & value);
        std:: string get(const std:: string &key);
        bool del(const std:: string &key);

    private:
        void maybeSnapshot();
        std::unordered_map<std:: string ,std:: string> data_;
        mutable std:: shared_mutex mutex_;
        WAL wal_;

        Snapshot snapshot_;
        int writeSinceSnapshot = 0;
        static constexpr int SNAPSHOT_THRESHOLD = 10000;
};

