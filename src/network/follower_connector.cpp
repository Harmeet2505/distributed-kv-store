#include "follower_connector.hpp"
#include<iostream>
#include<sstream>
#include<cstring>
#include<unistd.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<arpa/inet.h>

FollowerConnector:: FollowerConnector(KVStore &store, const std:: string &leaderIp , u_int16_t leaderPort) :store_(store), leaderIp_(leaderIp) ,leaderPort_(leaderPort) {}

void FollowerConnector:: run(){
    leaderFd_ = socket(AF_INET, SOCK_STREAM, 0);
    std::cout << "Attempting to connect to " << leaderIp_ << ":" << leaderPort_ << "\n";
    if (leaderFd_ < 0){
        std:: cerr << "Failed to make socket\n";
        return;
    }

    sockaddr_in leaderAddr{};
    leaderAddr.sin_family = AF_INET;
    leaderAddr.sin_port = htons(leaderPort_);
    int result = inet_pton(AF_INET , leaderIp_.c_str(), &leaderAddr.sin_addr);

    if (result <= 0){
        std::cerr << "Invalid address or address not supported: " << leaderIp_ << "\n";
        return;
    }

    if (connect(leaderFd_ , (sockaddr*) &leaderAddr , sizeof(leaderAddr)) < 0){
        std:: cerr << "Failed to connect to leader:" << strerror(errno) << '\n';
        return;
    }

    std:: cout << "connected to leader at :" << leaderIp_ << ":" << leaderPort_ << '\n';
    char buffer[1024];
    std:: string leftover;

    while (true){
        ssize_t bytesRead = recv(leaderFd_ , buffer, sizeof(buffer) -1 , 0);
        if (bytesRead <= 0){
            std:: cerr << "Disconnected from leader\n";
            break;
        } 

        buffer[bytesRead] = '\0';
        leftover += buffer;

        size_t pos;
        while ((pos = leftover.find('\n')) != std:: string:: npos){
            std:: string line = leftover.substr(0,pos);
            leftover.erase(0 , pos + 1);

            handleCommand(line);

            send(leaderFd_ , "ACK\n" , 4 , 0);
        }
    }
}

void FollowerConnector:: handleCommand(const std:: string &line){
    std:: istringstream iss(line);
    std:: string command, op , key , value;
    iss >> command >> op >> key;

    if (op == "SET"){
        iss >> value;
        store_.set(key , value);
    }
    else if (op == "DEL"){
        store_.del(key);
    }
} 
