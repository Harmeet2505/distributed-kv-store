#pragma once
#include "store/kv_store.hpp"
#include "persistence/raft_state.hpp"
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <atomic>

enum class Role { FOLLOWER, CANDIDATE, LEADER };

struct LogEntry {
    int term;
    std::string command;
};

struct PeerInfo {
    std::string id;
    std::string ip;
    uint16_t port;
    int fd = -1;  
};

class RaftNode {
public:
    RaftNode(const std::string& selfId, KVStore& store,const std::string& raftStatePath, int totalNodes);

    void addPeer(const std::string& id, const std::string& ip, uint16_t port);
    void start(); 

    bool clientSet(const std::string& key, const std::string& value);
    bool clientDel(const std::string& key);
    bool isLeader() const { return role_ == Role::LEADER; }

    std::string handleRequestVote(int term, const std::string& candidateId,int lastLogIndex, int lastLogTerm);
    std::string handleAppendEntries(int term, const std::string& leaderId,int prevLogIndex, int prevLogTerm,
        const std::vector<LogEntry>& entries,
        int leaderCommit);

    private:
        void electionTimerLoop();
        void leaderHeartbeatLoop();
        void connectionMaintenanceLoop(); 

        void startElection();
        void stepDown(int newTerm);
        void becomeLeader();
        void resetElectionTimer();
        bool hasElectionTimedOut();

        void sendHeartbeatsToAll();
        bool sendAppendEntriesTo(const std::string& peerId, int fd);
        bool ensureConnected(PeerInfo& peer);

        void applyToStateMachine(int index);  

        std::string selfId_;
        KVStore& store_;
        RaftState raftState_;
        int totalNodes_;

        std::mutex mutex_; 

        int currentTerm_ = 0;
        std::string votedFor_ = "";
        std::vector<LogEntry> log_;
        Role role_ = Role::FOLLOWER;

        int commitIndex_ = -1;
        int lastApplied_ = -1;

        std::unordered_map<std::string, int> nextIndex_;
        std::unordered_map<std::string, int> matchIndex_;
        std::unordered_map<std::string, PeerInfo> peers_;

        std::chrono::steady_clock::time_point lastHeartbeat_;
        int electionTimeoutMs_ = 150;

        std:: atomic<bool> running_{false};
};