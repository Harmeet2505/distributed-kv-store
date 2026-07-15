#include "kv_store.hpp"
#include "persistence/wal.hpp"

KVStore:: KVStore(const std:: string &walpath , const std:: string &snapshotPath): wal_(walpath) , snapshot_(snapshotPath){
    data_ = snapshot_.load();

    wal_.replay([this](const std:: string &op ,const std:: string &key , const std:: string &value){
        if (op == "SET"){
            data_[key] = value;
        }
        else if (op == "DEL"){
            data_.erase(key);
        }
    });
}

void KVStore:: set(const std:: string &key , const std:: string &value){
    std:: unique_lock lock(mutex_);
    wal_.logSet(key , value);
    data_[key] = value;
    writeSinceSnapshot++;
    maybeSnapshot();
}

std:: string KVStore:: get(const std:: string &key){
    std:: shared_lock lock(mutex_);

    if (data_.find(key) != data_.end()){
        return data_[key];
    }
    return "";
}

bool KVStore :: del(const std:: string &key){
    std :: unique_lock lock(mutex_);
    if (data_.find(key) != data_.end()){
        wal_.logDel(key);
        data_.erase(key);
        writeSinceSnapshot++;
        maybeSnapshot();
        return true;
    }

    return false;
}

void KVStore:: maybeSnapshot(){
    if (writeSinceSnapshot >= SNAPSHOT_THRESHOLD){
        snapshot_.save(data_);
        wal_.truncate();
        writeSinceSnapshot = 0;
    }
}



