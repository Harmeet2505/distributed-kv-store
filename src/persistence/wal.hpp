#pragma once
#include<fstream>
#include<functional>

class WAL {
    public:
        explicit WAL(const std:: string &filepath);
        ~WAL();

        // Append to log before applying to memory
        void logSet(const std:: string &key , const std:: string &value);
        void logDel(const std:: string &key);

        // Replay existing log on startup
        void replay(const std::function<void(const std:: string &op , const std:: string &key , const std:: string &value)> &callback);
        void truncate();
        
    private:
        std:: string filepath_;
        std:: ofstream logFile_;
};