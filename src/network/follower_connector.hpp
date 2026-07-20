#pragma once
#include "store/kv_store.hpp"
#include<string>
#include<cstdint>

class FollowerConnector{
    public:
        FollowerConnector(KVStore &store , const std:: string &leaderIp , uint16_t leaderPort);
        void run();

    private:
        void handleCommand(const std:: string &line);
        KVStore &store_;
        std:: string leaderIp_;
        uint16_t leaderPort_;
        int leaderFd_;
};