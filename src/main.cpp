#include<iostream>
#include<sstream>
#include "store/kv_store.hpp"
#include "network/server.hpp"
#include "persistence/wal.hpp"
#include "replication/replication_manager.hpp"
#include "network/follower_connector.hpp"
#include "network/raft_server.hpp"
#include "raft/raft_node.hpp"
#include <thread>


std:: unordered_map<std:: string,std:: string> parseArgs(int argc , char *argv[]){
    std:: unordered_map<std:: string, std:: string> args;

    for (int i = 1; i < argc ; i++){
        std:: string arg = argv[i];
        size_t eqPos = arg.find('=');

        if (arg.substr(0 , 2) == "--" && eqPos != std:: string:: npos){
            std:: string key = arg.substr(2 , eqPos - 2);
            std:: string value = arg.substr(eqPos + 1);
            args[key] = value;
        }
    }

    return args;
}

int main(int argc , char* argv[]){

    auto args = parseArgs(argc , argv);
    std::string selfId = args["node-id"];
    uint16_t clientPort = std::stoi(args["client-port"]);
    uint16_t raftPort = std::stoi(args["raft-port"]);
    int totalNodes = std::stoi(args["total-nodes"]);
    std::string walPath = args.count("wal-path") ? args["wal-path"] : "data/wal_" + selfId + ".log";
    std::string snapshotPath = args.count("snapshot-path") ? args["snapshot-path"] : "data/snapshot_" + selfId + ".dat";
    std::string raftStatePath = args.count("raft-state-path") ? args["raft-state-path"] : "data/raft_state_" + selfId + ".dat";

    KVStore store(walPath, snapshotPath);
    RaftNode raftNode(selfId, store, raftStatePath, totalNodes);

    if (args.count("peers")) {
        std::istringstream iss(args["peers"]);
        std::string peerStr;
        while (std::getline(iss, peerStr, ',')) {
            std::istringstream pss(peerStr);
            std::string id, ip, portStr;
            std::getline(pss, id, ':');
            std::getline(pss, ip, ':');
            std::getline(pss, portStr, ':');
            raftNode.addPeer(id, ip, std::stoi(portStr));
        }
    }

    raftNode.start();

    RaftServer raftServer(raftNode, raftPort);
    std::thread raftServerThread(&RaftServer::run, &raftServer);
    raftServerThread.detach();

    Server clientServer(store, clientPort, nullptr, &raftNode);
    clientServer.run();

    return 0;
}