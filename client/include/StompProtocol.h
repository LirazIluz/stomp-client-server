#pragma once
#include <iostream>
#include <stdio.h>
#include <atomic>
#include <mutex>
#include <thread>
#include <map>
#include <vector>
#include <stdexcept>
#include "../include/ConnectionHandler.h"
#include "../include/event.h"
#include "../include/Game.h"

using std::string; 
class StompProtocol
{
private:
    ConnectionHandler * ch;
    string _username;
    std::atomic<bool> shouldTerminateField;
    std::atomic<bool> isConnected;
    std::map<string,Game> Games;

public:

    StompProtocol();
    ~StompProtocol();
    void start(ConnectionHandler * con,string username);
    void handleLogin(string& hostInfo,string& username,string &password);
    void handleJoin(string& game,int subId,int receiptId);
    void handleExit(string& game,int subId,int receiptId);
    void handleLogout(int receiptId);
    void handleReport(string& filePath);
    void handleSummary(string& game,string& user,string& filePath);

   
    bool shouldTerminate();
    bool getIsConnected();
    void setShouldTerminate(bool status);
    void setIsConnected(bool status);
    void addUpdate(string& game,string& user,Event& e);
    
    Event parseToEvent(std::vector<string> lines);
    void parseResponse(std::map<int,string>& receipts);
    string ConstructEventFrame(Event& e);
};
