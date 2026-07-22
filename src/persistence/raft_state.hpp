#pragma once
#include <string>

class RaftState {
public:
    explicit RaftState(const std::string& filepath);

    void save(int currentTerm, const std::string& votedFor);
    void load(int& currentTerm, std::string& votedFor);

private:
    std::string filepath_;
};