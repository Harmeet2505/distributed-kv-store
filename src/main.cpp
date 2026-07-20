#include<iostream>
#include "store/kv_store.hpp"
#include "network/server.hpp"
#include "persistence/wal.hpp"
#include "replication/replication_manager.hpp"
#include "network/follower_connector.hpp"

std:: unordered_map<std:: string,std:: string> parseArgs(int argc , char *argv[]){
    std:: unordered_map<std:: string, std:: string> args;

    for (int i = 0; i < argc ; i++){
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

    std:: string role = args["role"];
    std:: string walPath = args.count("wal-path") ? args["wal-path"] : "data/wal.log";
    std:: string snapshotPath = args.count("snapshot-path") ? args["snapshot-path"] : "data/snapshot.dat";

    KVStore store(walPath, snapshotPath);
    if (role == "leader"){
        uint16_t clientPort = std:: stoi(args["client-port"]);
        uint16_t replicationPort = std:: stoi(args["replication-port"]);
        int totalNodes = std:: stoi(args["total-nodes"]);

        ReplicationManager replicationManager(replicationPort , totalNodes);
        replicationManager.start();

        Server server(store , clientPort , &replicationManager);
        server.run();
    }
    else if (role == "follower"){
        std::string leaderAddress = args["leader-address"];
        size_t colonPos = leaderAddress.find(':');
        std::string leaderIp = leaderAddress.substr(0, colonPos);
        uint16_t leaderPort = std::stoi(leaderAddress.substr(colonPos + 1));

        FollowerConnector connector(store, leaderIp, leaderPort);
        connector.run(); 
    }
    else {
        std:: cerr << "unknow role\n";
        return 1;
    }

    return 0;
}