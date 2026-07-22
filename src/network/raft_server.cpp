#include "network/raft_server.hpp"
#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

RaftServer:: RaftServer(RaftNode& node, uint16_t port) : node_(node), port_(port), serverFd_(-1) {}

void RaftServer::run() {
    serverFd_ = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(serverFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(serverFd_, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "Raft server bind failed\n"; return;
    }
    if (listen(serverFd_, 10) < 0) {
        std::cerr << "Raft server listen failed\n"; return;
    }
    std::cout << "Raft server listening on port " << port_ << "\n";

    while (true) {
        int fd = accept(serverFd_, nullptr, nullptr);
        if (fd < 0) continue;
        std::thread(&RaftServer::handlePeer, this, fd).detach();
    }
}

void RaftServer::handlePeer(int fd) {
    char buffer[4096];
    std::string leftover;

    while (true) {
        ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
        if (n <= 0) break;
        buffer[n] = '\0';
        leftover += buffer;

        size_t pos;
        while ((pos = leftover.find('\n')) != std::string::npos) {
            std::string line = leftover.substr(0, pos);
            leftover.erase(0, pos + 1);

            std::istringstream iss(line);
            std::string type;
            iss >> type;

            std::string response;
            if (type == "REQUEST_VOTE") {
                int term, lastLogIndex, lastLogTerm;
                std::string candidateId;
                iss >> term >> candidateId >> lastLogIndex >> lastLogTerm;
                response = node_.handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm);
            } else if (type == "APPEND_ENTRIES") {
                int term, prevLogIndex, prevLogTerm, leaderCommit, entryCount;
                std::string leaderId;
                iss >> term >> leaderId >> prevLogIndex >> prevLogTerm >> leaderCommit >> entryCount;

                std::vector<LogEntry> entries;
                for (int i = 0; i < entryCount; i++) {
                    std::string token;
                    iss >> token;
                    size_t firstBar = token.find('|', 1);
                    int entryTerm = std::stoi(token.substr(1, firstBar - 1));
                    std::string command = token.substr(firstBar + 1);
                    std::replace(command.begin(), command.end(), '~', ' ');  // restore spaces
                    entries.push_back({entryTerm, command});
                }
                response = node_.handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
            } else {
                response = "UNKNOWN";
            }

            response += "\n";
            send(fd, response.c_str(), response.size(), 0);
        }
    }
    close(fd);
}