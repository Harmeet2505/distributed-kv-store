#include "persistence/raft_state.hpp"
#include <fstream>
#include <cstdio>
#include <iostream>
#include <sstream>

RaftState::RaftState(const std::string& filepath) : filepath_(filepath) {}

void RaftState::save(int currentTerm, const std::string& votedFor) {
    std::string tempPath = filepath_ + ".tmp";
    {
        std::ofstream out(tempPath, std::ios::trunc);
        if (!out.is_open()) {
            std::cerr << "Failed to open raft state temp file\n";
            return;
        }
        out << currentTerm << "\n" << votedFor << "\n";
        out.flush();
    }
    if (std::rename(tempPath.c_str(), filepath_.c_str()) != 0) {
        std::cerr << "Failed to persist raft state\n";
    }
}

void RaftState::load(int& currentTerm, std::string& votedFor) {
    std::ifstream in(filepath_);
    if (!in.is_open()) {
        currentTerm = 0;
        votedFor = "";
        return;
    }
    in >> currentTerm;
    in.ignore();
    std::getline(in, votedFor);
}