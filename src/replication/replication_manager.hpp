#pragma once
#include<vector>
#include<mutex>
#include <cstdint>

class ReplicationManager{
    public:
        ReplicationManager(uint16_t replicationPort , int totalNodes);
        void start();
        void replicate(const std:: string &command);

    private:
        void acceptLoop();
        bool waitForAck(int followerFd);

        uint16_t replicationPort_;
        int totalNodes_;
        int serverFd_;

        std:: vector<int> followerSockets_;
        std:: mutex followersMutex_;
};

