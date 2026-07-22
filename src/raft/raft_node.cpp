#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "raft/raft_node.hpp"

RaftNode::RaftNode(const std::string& selfId, KVStore& store,const std::string& raftStatePath, int totalNodes)
    : selfId_(selfId), store_(store), raftState_(raftStatePath), totalNodes_(totalNodes) {
    raftState_.load(currentTerm_, votedFor_);
}

void RaftNode::addPeer(const std::string& id, const std::string& ip, uint16_t port) {
    PeerInfo p;
    p.id = id;
    p.ip = ip;
    p.port = port;
    peers_[id] = p;
}

void RaftNode::start() {
    running_ = true;
    resetElectionTimer();
    std::thread(&RaftNode::electionTimerLoop, this).detach();
    std::thread(&RaftNode::connectionMaintenanceLoop, this).detach();
}

void RaftNode::resetElectionTimer() {
    lastHeartbeat_ = std::chrono::steady_clock::now();
    electionTimeoutMs_ = 150 + (rand() % 150);
}

bool RaftNode::hasElectionTimedOut() {
    auto elapsed = std::chrono::steady_clock::now() - lastHeartbeat_;
    return elapsed > std::chrono::milliseconds(electionTimeoutMs_);
}

void RaftNode::electionTimerLoop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::LEADER && hasElectionTimedOut()) {
            startElection();
        }
    }
}

bool RaftNode::ensureConnected(PeerInfo& peer) {
    if (peer.fd >= 0) return true;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;

    struct timeval timeout{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.ip.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return false;
    }
    peer.fd = fd;
    return true;
}

void RaftNode::connectionMaintenanceLoop() {
    while (running_) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [id, peer] : peers_) {
                if (peer.fd < 0) {
                    ensureConnected(peer);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

static bool sendLine(int fd, const std::string& msg) {
    std::string out = msg + "\n";
    return send(fd, out.c_str(), out.size(), 0) > 0;
}

static bool recvLine(int fd, std::string& line) {
    char buffer[1024];
    ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) return false;
    buffer[n] = '\0';
    line = buffer;
    size_t pos = line.find('\n');
    if (pos != std::string::npos) line = line.substr(0, pos);
    return true;
}

void RaftNode::startElection() {
    currentTerm_++;
    role_ = Role::CANDIDATE;
    votedFor_ = selfId_;
    raftState_.save(currentTerm_, votedFor_);
    resetElectionTimer();

    int term = currentTerm_;
    int votesReceived = 1;
    int votesNeeded = (totalNodes_ / 2) + 1;

    int lastLogIndex = (int)log_.size() - 1;
    int lastLogTerm = log_.empty() ? 0 : log_.back().term;

    std::cout << "[" << selfId_ << "] Starting election for term " << term << "\n";

    for (auto& [peerId, peer] : peers_) {
        if (peer.fd < 0 && !ensureConnected(peer)) continue;

        std::string request = "REQUEST_VOTE " + std::to_string(term) + " " + selfId_ +
                            " " + std::to_string(lastLogIndex) + " " + std::to_string(lastLogTerm);
        if (!sendLine(peer.fd, request)) { close(peer.fd); peer.fd = -1; continue; }

        std::string response;
        if (!recvLine(peer.fd, response)) { close(peer.fd); peer.fd = -1; continue; }

        std::istringstream iss(response);
        std::string verdict;
        int responseTerm;
        iss >> verdict >> responseTerm;

        if (responseTerm > currentTerm_) {
            stepDown(responseTerm);
            return;
        }
        if (verdict == "VOTE_GRANTED" && role_ == Role::CANDIDATE && currentTerm_ == term) {
            votesReceived++;
            if (votesReceived >= votesNeeded) {
                becomeLeader();
                return;
            }
        }
    }
}

void RaftNode::stepDown(int newTerm) {
    currentTerm_ = newTerm;
    role_ = Role::FOLLOWER;
    votedFor_ = "";
    raftState_.save(currentTerm_, votedFor_);
    resetElectionTimer();
    std::cout << "[" << selfId_ << "] Stepping down to follower, term " << newTerm << "\n";
}

void RaftNode::becomeLeader() {
    role_ = Role::LEADER;
    std::cout << "[" << selfId_ << "] Became leader for term " << currentTerm_ << "\n";
    int nextIdx = (int)log_.size();
    for (auto& [peerId, peer] : peers_) {
        nextIndex_[peerId] = nextIdx;
        matchIndex_[peerId] = -1;
    }
    sendHeartbeatsToAll();
    std::thread(&RaftNode::leaderHeartbeatLoop, this).detach();
}

void RaftNode::leaderHeartbeatLoop() {
    while (running_) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (role_ != Role::LEADER) return;
            sendHeartbeatsToAll();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

void RaftNode::sendHeartbeatsToAll() {
    for (auto& [peerId, peer] : peers_) {
        if (peer.fd < 0 && !ensureConnected(peer)) continue;
        sendAppendEntriesTo(peerId, peer.fd);
    }
}

bool RaftNode::sendAppendEntriesTo(const std::string& peerId, int fd) {
    int nextIdx = nextIndex_[peerId];
    int prevLogIndex = nextIdx - 1;
    int prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < (int)log_.size()) ? log_[prevLogIndex].term : 0;

    std::ostringstream oss;
    oss << "APPEND_ENTRIES " << currentTerm_ << " " << selfId_ << " "
        << prevLogIndex << " " << prevLogTerm << " " << commitIndex_ << " ";

    int entryCount = (int)log_.size() - nextIdx;
    if (entryCount < 0) entryCount = 0;
    oss << entryCount;

    for (int i = nextIdx; i < (int)log_.size(); i++) {
        std::string encodedCommand = log_[i].command;
        std::replace(encodedCommand.begin(), encodedCommand.end(), ' ', '~');
        oss << " |" << log_[i].term << "|" << encodedCommand;
    }

    if (!sendLine(fd, oss.str())) { close(fd); peers_[peerId].fd = -1; return false; }

    std::string response;
    if (!recvLine(fd, response)) { close(fd); peers_[peerId].fd = -1; return false; }

    std::istringstream iss(response);
    std::string verdict;
    int responseTerm;
    iss >> verdict >> responseTerm;

    if (responseTerm > currentTerm_) {
        stepDown(responseTerm);
        return false;
    }

    if (verdict == "APPEND_SUCCESS") {
        matchIndex_[peerId] = (int)log_.size() - 1;
        nextIndex_[peerId] = (int)log_.size();

        std::vector<int> matches;
        matches.push_back((int)log_.size() - 1);  // leader itself
        for (auto& [id, idx] : matchIndex_) matches.push_back(idx);
        std::sort(matches.begin(), matches.end(), std::greater<int>());
        int majorityMatch = matches[totalNodes_ / 2];
        if (majorityMatch > commitIndex_ && majorityMatch >= 0 &&
            log_[majorityMatch].term == currentTerm_) {
            commitIndex_ = majorityMatch;
            while (lastApplied_ < commitIndex_) {
                lastApplied_++;
                applyToStateMachine(lastApplied_);
            }
        }
    } else {
        if (nextIndex_[peerId] > 0) nextIndex_[peerId]--;
    }
    return true;
}

void RaftNode::applyToStateMachine(int index) {
    std::istringstream iss(log_[index].command);
    std::string op, key, value;
    iss >> op >> key;
    if (op == "SET") { iss >> value; store_.set(key, value); }
    else if (op == "DEL") { store_.del(key); }
}

bool RaftNode::clientSet(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ != Role::LEADER) return false;
    log_.push_back({currentTerm_, "SET " + key + " " + value});
    return true;  
}

bool RaftNode::clientDel(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (role_ != Role::LEADER) return false;
    log_.push_back({currentTerm_, "DEL " + key});
    return true;
}

std::string RaftNode::handleRequestVote(int term, const std::string& candidateId,int lastLogIndex, int lastLogTerm) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (term < currentTerm_) {
        return "VOTE_DENIED " + std::to_string(currentTerm_);
    }
    if (term > currentTerm_) {
        stepDown(term);
    }

    bool canVote = (votedFor_.empty() || votedFor_ == candidateId);
    int myLastLogIndex = (int)log_.size() - 1;
    int myLastLogTerm = log_.empty() ? 0 : log_.back().term;
    bool logOk = (lastLogTerm > myLastLogTerm) || (lastLogTerm == myLastLogTerm && lastLogIndex >= myLastLogIndex);

    if (canVote && logOk) {
        votedFor_ = candidateId;
        raftState_.save(currentTerm_, votedFor_);
        resetElectionTimer();
        return "VOTE_GRANTED " + std::to_string(currentTerm_);
    }
    return "VOTE_DENIED " + std::to_string(currentTerm_);
}

std::string RaftNode::handleAppendEntries(int term, const std::string& leaderId,
                                        int prevLogIndex, int prevLogTerm,
                                        const std::vector<LogEntry>& entries,
                                        int leaderCommit) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (term < currentTerm_) {
        return "APPEND_FAIL " + std::to_string(currentTerm_);
    }
    if (term >= currentTerm_) {
        currentTerm_ = term;
        role_ = Role::FOLLOWER;
        resetElectionTimer();
    }

    if (prevLogIndex >= 0) {
        if (prevLogIndex >= (int)log_.size() || log_[prevLogIndex].term != prevLogTerm) {
            return "APPEND_FAIL " + std::to_string(currentTerm_);
        }
    }

    int idx = prevLogIndex + 1;
    for (auto& entry : entries) {
        if (idx < (int)log_.size()) {
            if (log_[idx].term != entry.term) {
                log_.resize(idx);
                log_.push_back(entry);
            }
        } else {
            log_.push_back(entry);
        }
        idx++;
    }

    if (leaderCommit > commitIndex_) {
        commitIndex_ = std::min(leaderCommit, (int)log_.size() - 1);
        while (lastApplied_ < commitIndex_) {
            lastApplied_++;
            applyToStateMachine(lastApplied_);
        }
    }

    return "APPEND_SUCCESS " + std::to_string(currentTerm_);
}