#include "kv_store.hpp"

void KVStore:: set(const std:: string &key , const std:: string &value){
    std:: unique_lock lock(mutex_);
    data_[key] = value;
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
        data_.erase(key);
        return true;
    }

    return false;
}



