#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

struct NodeAddr { std::string ip; uint16_t port; };

int connectTo(const NodeAddr& node) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct timeval timeout{2, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(node.port);
    inet_pton(AF_INET, node.ip.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

std::string sendCommand(int fd, const std::string& command) {
    std::string msg = command + "\n";
    if (send(fd, msg.c_str(), msg.size(), 0) <= 0) return "";

    char buffer[1024];
    ssize_t n = recv(fd, buffer, sizeof(buffer) - 1, 0);
    if (n <= 0) return "";
    buffer[n] = '\0';
    return std::string(buffer);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: kv_client <ip1:port1,ip2:port2,...>\n";
        return 1;
    }

    std::vector<NodeAddr> nodes;
    std::istringstream nodeStream(argv[1]);
    std::string entry;
    while (std::getline(nodeStream, entry, ',')) {
        size_t colon = entry.find(':');
        nodes.push_back({entry.substr(0, colon), (uint16_t)std::stoi(entry.substr(colon + 1))});
    }

    int currentFd = -1;
    int currentNodeIdx = -1;

    std::cout << "Connected to KV cluster. Type commands (SET/GET/DEL). Ctrl+C to quit.\n";

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;

        std::string response;
        int attempts = 0;

        while (attempts < (int)nodes.size() * 2) { 
            if (currentFd < 0) {
                bool connected = false;
                for (size_t i = 0; i < nodes.size(); i++) {
                    int idx = (currentNodeIdx + 1 + (int)i) % nodes.size();
                    int fd = connectTo(nodes[idx]);
                    if (fd >= 0) {
                        currentFd = fd;
                        currentNodeIdx = idx;
                        connected = true;
                        break;
                    }
                }
                if (!connected) {
                    std::cerr << "Could not reach any node in the cluster\n";
                    break;
                }
            }

            response = sendCommand(currentFd, line);

            if (response.empty()) {
                close(currentFd);
                currentFd = -1;
                attempts++;
                continue;
            }

            if (response.find("ERROR not leader") != std::string::npos) {
                close(currentFd);
                currentFd = -1;
                attempts++;
                continue;
            }

            break;  
        }

        if (!response.empty()) {
            std::cout << response;
        } else {
            std::cerr << "Failed to get a response from the cluster\n";
        }
    }

    if (currentFd >= 0) close(currentFd);
    return 0;
}