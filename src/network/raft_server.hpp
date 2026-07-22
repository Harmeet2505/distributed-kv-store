#pragma once
#include "raft/raft_node.hpp"
#include <cstdint>

class RaftServer {
public:
    RaftServer(RaftNode& node, uint16_t port);
    void run();  

private:
    void handlePeer(int fd);
    RaftNode& node_;
    uint16_t port_;
    int serverFd_;
};