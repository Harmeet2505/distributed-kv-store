#include<iostream>
#include "store/kv_store.hpp"
#include "network/server.hpp"
#include "persistence/wal.hpp"


int main(){
    KVStore store("../data/wal.log");
    Server server(store , 8080);

    server.run();
}