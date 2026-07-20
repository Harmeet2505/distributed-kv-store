#include "replication_manager.hpp"
#include <iostream>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

ReplicationManager:: ReplicationManager(uint16_t replicationPort , int totalNodes) : replicationPort_(replicationPort) , totalNodes_(totalNodes) , serverFd_(-1) {}

void ReplicationManager::start() {
    serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd_ < 0){
        std:: cerr<< "Failed to create replication socket\n";
        return;
    }

    int opt = 1;
    setsockopt(serverFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(replicationPort_);

    if (bind(serverFd_, (sockaddr*)&address, sizeof(address)) < 0){
        std :: cerr << "Replication Bind Failed :" << strerror(errno) << '\n';
        return;
    }

    if (listen(serverFd_, 10) < 0) {
        std::cerr << "Replication listen failed: " << strerror(errno) << "\n";
        return;
    }

    std::cout << "Replication manager listening on port " << replicationPort_ << "\n";

    std::thread(&ReplicationManager::acceptLoop, this).detach();
}

void ReplicationManager::acceptLoop() {
    std::cout << "Accept loop started, waiting for followers...\n";

    while (true) {
        int followerFd = accept(serverFd_, nullptr, nullptr);
        if (followerFd < 0) continue;

        struct timeval timeout;
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(followerFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        std::lock_guard<std::mutex> lock(followersMutex_);
        followerSockets_.push_back(followerFd);
        std::cout << "Follower connected\n";
    }
}

void ReplicationManager::replicate(const std:: string &command){
    std:: lock_guard<std::mutex> lock(followersMutex_);
    std::cout << "Replicating to " << followerSockets_.size() << " followers\n";

    std:: string msg = command + "\n";

    int ackNeeds = totalNodes_ / 2;
    int ackRecieved = 0;
    
    std:: vector<int> stillConnected;
    for (int fd : followerSockets_){
        ssize_t sent = send(fd ,msg.c_str() ,msg.size(), 0);

        if (sent <= 0){
            std:: cerr << "Follower disconnected during send , removing\n";
            close(fd);
            continue;
        }
        stillConnected.push_back(fd);
    }

    for (int fd : stillConnected){
        if (waitForAck(fd)){
            ackRecieved++;
            if (ackRecieved >= ackNeeds)
                break;
        }
        else {
            std :: cerr << "Follower Disconnected or timed out waiting for ack , removing\n";
            close(fd);
            continue;
        }
    } 
    followerSockets_ = stillConnected;
}

bool ReplicationManager ::waitForAck(int followerFd){
    char buffer[16];
    ssize_t byteRead = recv(followerFd , buffer , sizeof(buffer) - 1 , 0);
    
    buffer[byteRead] = '\0';
    std:: string response(buffer);

    return response.find("ACK") != std:: string:: npos;
}