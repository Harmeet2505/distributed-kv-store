#include "snapshot.hpp"
#include<fstream>
#include<cstdio>
#include<iostream>

Snapshot:: Snapshot(const std:: string &filepath): filepath_(filepath){}

void Snapshot:: save(const std:: unordered_map<std:: string ,std:: string> &data_){
    std:: string tempPath = filepath_ + ".tmp";
    
    std:: ofstream tempFile(tempPath , std:: ios:: trunc);
    if (!tempFile.is_open()){
        std:: cerr << "Failed to open temp sanpshot\n";
        return;
    }

    for (const auto&[key , value] : data_){
        tempFile << key << "=" << value << '\n';
    }
    tempFile.flush();

    if (std::rename(tempPath.c_str() , filepath_.c_str()) != 0){
        std:: cerr << "Failed to rename temp snapshot to final snapshot\n";
    }
}


std:: unordered_map<std:: string, std:: string> Snapshot:: load(){
    std:: unordered_map<std:: string, std:: string> data;
    std:: ifstream inFile(filepath_);

    if (!inFile.is_open()){
        return data;
    }

    std:: string line;
    while (std:: getline(inFile , line)){
        size_t eqPos = line.find('=');
        if (eqPos == std::string::npos) continue;

        std:: string key = line.substr(0,eqPos);
        std:: string value = line.substr(eqPos + 1);
        data[key] = value;
    }

    return data;
}

