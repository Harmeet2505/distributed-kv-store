#pragma once
#include<cstdint>
#include "../store/kv_store.hpp"
#include "replication/replication_manager.hpp"
#include "raft/raft_node.hpp"

class Server {
    public:
        Server(KVStore &store , uint16_t port , ReplicationManager *replicationManager = nullptr ,
        RaftNode *raftNode = nullptr);
        void run();

    private:
        void handleClient(int client_fd);
        std:: string processCommand(const std:: string &line);

        KVStore &store_;
        uint16_t port_;
        int server_fd_;
        ReplicationManager* replicationManager_;
        RaftNode* raftNode_;
};