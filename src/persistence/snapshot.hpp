#pragma once
#include<string>
#include<unordered_map>

class Snapshot{
    public:
        explicit Snapshot(const std:: string &filepath);

        void save(const std:: unordered_map<std:: string, std:: string> &data_);
        std:: unordered_map<std:: string, std:: string> load();
    
    private:
        std:: string filepath_;

};