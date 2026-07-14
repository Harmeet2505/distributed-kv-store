#include "wal.hpp"
#include<iostream>
#include<sstream>
#include<filesystem>

WAL:: WAL(const std:: string &filepath) : filepath_(filepath){
    logFile_.open(filepath_ , std:: ios::app);
    if (!logFile_.is_open()){
        std:: cerr << "Failed to open WAL file\n";
    }
}

WAL:: ~WAL(){
    if (logFile_.is_open()){
        logFile_.close();
    }
}

void WAL:: logSet(const std:: string &key,const std:: string &value){
    logFile_ << "SET" << ' ' << key << ' ' << value << std::endl;
}

void WAL:: logDel(const std:: string &key){
    logFile_ << "DEL" << ' ' << key << std:: endl;
}

void WAL:: replay(const std::function<void(const std:: string &op , const std:: string &key , const std:: string &value)> &callback){
    std:: ifstream inFile(filepath_);
    if (!inFile.is_open()){
        return;
    }   

    std:: string line;
    while (std:: getline(inFile , line)){
        std:: istringstream iss(line);
        std:: string op , key, value;

        iss >> op >> key;
        if (op == "SET"){
            iss >> value;
            callback("SET",key, value);
        }
        else if (op == "DEL") {
            callback("DEL", key, "");
        }
    }
}







