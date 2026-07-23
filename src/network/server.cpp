#include "server.hpp"
#include<cstring>
#include<iostream>
#include<sstream>
#include<thread>
#include<unistd.h>
#include<sys/socket.h>
#include<netinet/in.h>


Server :: Server(KVStore &store ,uint16_t port , ReplicationManager* replicationManager , RaftNode *raftNode) :
    store_(store) , port_(port) , server_fd_(-1) , replicationManager_(replicationManager) , raftNode_(raftNode){}


void Server:: run(){
    //Socket = AF_INET (IPv4) , SOCK_STREAM (TCP)
    server_fd_ = socket(AF_INET, SOCK_STREAM , 0);

    if (server_fd_ < 0){
        std:: cerr << "Failed to create a Socket\n";
        return;
    }

    // Reuse the same address even if used recently
    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);

    // Socket with address and port
    if (bind(server_fd_ , (sockaddr*)&address, sizeof(address)) < 0){
        std:: cerr << "Bind Failed\n";
        return;
    }

    if (listen(server_fd_ , 10) < 0){
        std:: cerr << "Listen Failed\n";
        return ;
    }
    std::cout << "Server Listening on Port" << port_ << '\n';

    while (true){
        int client_fd = accept(server_fd_ , nullptr , nullptr);
        if (client_fd < 0){
            std:: cerr << "Accept Failed\n";
            continue;
        } 
        
        std::thread(&Server:: handleClient, this , client_fd).detach();
    }
}


void Server:: handleClient(int client_fd){
    char buffer[1024];
    std:: string leftover;

    while (true){
        // handle received data
        ssize_t byteRead = recv(client_fd , buffer , sizeof(buffer) - 1 ,0);
        if (byteRead <= 0) break;

        buffer[byteRead] = '\0';

        leftover += buffer;
        size_t pos;
        while ((pos = leftover.find('\n')) != std::string ::npos){
            std:: string line = leftover.substr(0,pos);
            leftover.erase(0,pos + 1);

            std:: string response = processCommand(line);
            send(client_fd , response.c_str() ,response.size(),0);
        }

    }
    close(client_fd);
}

std::string Server::processCommand(const std::string& line) {
    std::istringstream iss(line);
    std::string cmd, key, value;
    iss >> cmd >> key;

    if (raftNode_) {
        if (!raftNode_->isLeader()) {
            return "ERROR not leader\n";
        }
        if (cmd == "SET") {
            iss >> value;
            raftNode_->clientSet(key, value);
            return "OK\n";
        } else if (cmd == "GET") {
            std::string val = store_.get(key);
            return (val.empty() ? "(nil)\n" : val + "\n");
        } else if (cmd == "DEL") {
            bool committed = raftNode_->clientDel(key);
            return committed ? "OK\n" : "Error failed to commit\n";
        }
        return "ERROR unknown command\n";
    }

    if (cmd == "SET") {
        iss >> value;
        store_.set(key, value);
        raftNode_->clientSet(key, value);

        if (replicationManager_) replicationManager_->replicate("REPLICATE SET " + key + " " + value);
        return "OK\n";
    } else if (cmd == "GET") {
        std::string val = store_.get(key);
        return (val.empty() ? "(nil)\n" : val + "\n");
    } else if (cmd == "DEL") {
        bool deleted = store_.del(key);
        if (deleted && replicationManager_) replicationManager_->replicate("REPLICATE DEL " + key);
        return (deleted ? "OK\n" : "(nil)\n");
    }
    return "ERROR unknown command\n";
}