#pragma once
#include<cstdint>
#include "../store/kv_store.hpp"

class Server {
    public:
        Server(KVStore &store , uint16_t port);
        void run();

    private:
        void handleClient(int client_fd);
        std:: string processCommand(const std:: string &line);

        KVStore &store_;
        uint16_t port_;
        int server_fd_;
};