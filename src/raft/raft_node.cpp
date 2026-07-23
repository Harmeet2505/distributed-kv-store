#include "raft/raft_node.hpp"
#include <iostream>
#include <sstream>
#include <thread>
#include <cstring>
#include <cstdlib>
#include <algorithm>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

RaftNode::RaftNode(const std::string& selfId, KVStore& store,
                    const std::string& raftStatePath, int totalNodes)
    : selfId_(selfId), store_(store), raftState_(raftStatePath), totalNodes_(totalNodes) {
    raftState_.load(currentTerm_, votedFor_);
}

void RaftNode::addPeer(const std::string& id, const std::string& ip, uint16_t port) {
    PeerInfo p; p.id = id; p.ip = ip; p.port = port;
    std::lock_guard<std::mutex> lock(mutex_);
    peers_[id] = p;
    peerSendMutexes_[id] = std::make_unique<std::mutex>();
}

void RaftNode::start() {
    running_ = true;
    { std::lock_guard<std::mutex> lock(mutex_); resetElectionTimer(); }
    std::thread(&RaftNode::electionTimerLoop, this).detach();
    std::thread(&RaftNode::connectionMaintenanceLoop, this).detach();
}

// Called only while holding mutex_
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
        bool shouldStart = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            shouldStart = (role_ != Role::LEADER && hasElectionTimedOut());
        }
        if (shouldStart) startElection();
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
    std::cerr << "[" << selfId_ << "] Connected to peer at " << peer.ip << ":" << peer.port << "\n";


    peer.fd = fd;
    return true;
}

void RaftNode::connectionMaintenanceLoop() {
    while (running_) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& [id, peer] : peers_) {
                if (peer.fd < 0) ensureConnected(peer);
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
    char buffer[4096];
    ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) return false;
    buffer[n] = '\0';
    line = buffer;
    size_t pos = line.find('\n');
    if (pos != std::string::npos) line = line.substr(0, pos);
    return true;
}

void RaftNode::startElection() {
    int term, lastLogIndex, lastLogTerm, votesNeeded;
    std::vector<std::string> peerIds;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        currentTerm_++;
        role_ = Role::CANDIDATE;
        votedFor_ = selfId_;
        raftState_.save(currentTerm_, votedFor_);
        resetElectionTimer();

        term = currentTerm_;
        lastLogIndex = (int)log_.size() - 1;
        lastLogTerm = log_.empty() ? 0 : log_.back().term;
        votesNeeded = (totalNodes_ / 2) + 1;
        for (auto& [id, peer] : peers_) peerIds.push_back(id);

        std::cout << "[" << selfId_ << "] Starting election for term " << term << "\n";
    }

    int votesReceived = 1;
    std::string request = "REQUEST_VOTE " + std::to_string(term) + " " + selfId_ + " " + std::to_string(lastLogIndex) + " " + std::to_string(lastLogTerm);

    for (auto& peerId : peerIds) {
        int fd = -1;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (role_ != Role::CANDIDATE || currentTerm_ != term) return;
            auto it = peers_.find(peerId);
            if (it == peers_.end()) continue;
            if (!ensureConnected(it->second)) continue;
            fd = it->second.fd;
        }

        bool ok = sendLine(fd, request);
        std::string response;
        if (ok) ok = recvLine(fd, response);

        if (!ok) {
            std::lock_guard<std::mutex> lock(mutex_);
            close(fd);
            if (peers_.count(peerId)) peers_[peerId].fd = -1;
            continue;
        }

        std::istringstream iss(response);
        std::string verdict;
        int responseTerm = 0;
        iss >> verdict >> responseTerm;
        std::cerr << "[" << selfId_ << "] Got response from " << peerId << ": " << verdict << " term=" << responseTerm << "\n";

        std::lock_guard<std::mutex> lock(mutex_);
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
    std::thread(&RaftNode::leaderHeartbeatLoop, this).detach();
}

void RaftNode::leaderHeartbeatLoop() {
    while (running_) {
        bool stillLeader;
        std::vector<std::string> peerIds;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stillLeader = (role_ == Role::LEADER);
            if (stillLeader) for (auto& [id, peer] : peers_) peerIds.push_back(id);
        }
        if (!stillLeader) return;

        for (auto& peerId : peerIds) {
            sendAppendEntriesTo(peerId);  
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

bool RaftNode::sendAppendEntriesTo(const std::string& peerId) {
    std::mutex* peerLock;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (peerSendMutexes_.find(peerId) == peerSendMutexes_.end()) return false;
        peerLock = peerSendMutexes_[peerId].get();
    }

    std::lock_guard<std::mutex> sendLock(*peerLock); 

    int fd, term, prevLogIndex, prevLogTerm, leaderCommit;
    std::vector<LogEntry> entries;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::LEADER) return false;
        auto it = peers_.find(peerId);
        if (it == peers_.end()) return false;
        if (!ensureConnected(it->second)) return false;
        fd = it->second.fd;

        term = currentTerm_;
        int nextIdx = nextIndex_[peerId];
        prevLogIndex = nextIdx - 1;
        prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < (int)log_.size()) ? log_[prevLogIndex].term : 0;
        leaderCommit = commitIndex_;
        for (int i = nextIdx; i < (int)log_.size(); i++) entries.push_back(log_[i]);
    }

    std::ostringstream oss;
    oss << "APPEND_ENTRIES " << term << " " << selfId_ << " "
        << prevLogIndex << " " << prevLogTerm << " " << leaderCommit << " " << entries.size();
    for (auto& e : entries) {
        std::string encoded = e.command;
        std::replace(encoded.begin(), encoded.end(), ' ', '~');
        oss << " |" << e.term << "|" << encoded;
    }

    bool ok = sendLine(fd, oss.str());
    std::string response;
    if (ok) ok = recvLine(fd, response);

    if (!ok) {
        std::lock_guard<std::mutex> lock(mutex_);
        close(fd);
        if (peers_.count(peerId)) peers_[peerId].fd = -1;
        return false;
    }

    std::istringstream iss(response);
    std::string verdict;
    int responseTerm = 0;
    iss >> verdict >> responseTerm;

    std::lock_guard<std::mutex> lock(mutex_);
    if (responseTerm > currentTerm_) { stepDown(responseTerm); return false; }
    if (currentTerm_ != term || role_ != Role::LEADER) return false;

    if (verdict == "APPEND_SUCCESS") {
        matchIndex_[peerId] = prevLogIndex + (int)entries.size();
        nextIndex_[peerId] = matchIndex_[peerId] + 1;

        std::vector<int> matches;
        matches.push_back((int)log_.size() - 1);
        for (auto& [id, idx] : matchIndex_) matches.push_back(idx);
        std::sort(matches.begin(), matches.end(), std::greater<int>());
        int majorityMatch = matches[totalNodes_ / 2];
        if (majorityMatch > commitIndex_ && majorityMatch >= 0 && majorityMatch < (int)log_.size() &&
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
    int logIndex;
    std::vector<std::string> peerIds;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::LEADER) return false;
        log_.push_back({currentTerm_, "SET " + key + " " + value});
        logIndex = (int)log_.size() - 1;
        for (auto& [id, peer] : peers_) peerIds.push_back(id);
    }

    for (auto& peerId : peerIds) {
        sendAppendEntriesTo(peerId);
    }

    auto start = std::chrono::steady_clock::now();
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (commitIndex_ >= logIndex) return true;
            if (role_ != Role::LEADER) return false;
        }
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));  // tighter poll now that we're not heartbeat-bound
    }
}

bool RaftNode::clientDel(const std::string& key) {
    int logIndex;
    std::vector<std::string> peerIds;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::LEADER) return false;
        log_.push_back({currentTerm_, "DEL " + key});
        logIndex = (int)log_.size() - 1;
        for (auto& [id, peer] : peers_) peerIds.push_back(id);
    }

    for (auto& peerId : peerIds) {
        sendAppendEntriesTo(peerId);
    }

    auto start = std::chrono::steady_clock::now();
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (commitIndex_ >= logIndex) return true;
            if (role_ != Role::LEADER) return false;
        }
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

std::string RaftNode::handleRequestVote(int term, const std::string& candidateId,int lastLogIndex, int lastLogTerm) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (term < currentTerm_) return "VOTE_DENIED " + std::to_string(currentTerm_);
    if (term > currentTerm_) stepDown(term);

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

std::string RaftNode::handleAppendEntries(int term, const std::string& leaderId,int prevLogIndex, int prevLogTerm,
                                    const std::vector<LogEntry>& entries,
                                    int leaderCommit) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (term < currentTerm_) return "APPEND_FAIL " + std::to_string(currentTerm_);

    currentTerm_ = term;
    role_ = Role::FOLLOWER;
    resetElectionTimer();

    if (prevLogIndex >= 0) {
        if (prevLogIndex >= (int)log_.size() || log_[prevLogIndex].term != prevLogTerm) {
            return "APPEND_FAIL " + std::to_string(currentTerm_);
        }
    }

    int idx = prevLogIndex + 1;
    for (auto& entry : entries) {
        if (idx < (int)log_.size()) {
            if (log_[idx].term != entry.term) { log_.resize(idx); log_.push_back(entry); }
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