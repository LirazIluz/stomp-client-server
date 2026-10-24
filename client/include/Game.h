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

using std::string; 
class Game
{
    private:
        string name;
        std::map<string,std::vector<Event>> updateByUserBefore;
        std::map<string,std::vector<Event>> updateByUserAfter;
        std::map<string,bool> halfTimeFalgs;
        std::map<string,bool> isDone;

    public:
        Game() : name("") {};
        Game(string name):name(name){};

        string getName(){return name;}
        std::vector<Event> getUpdatesBefore(string& user){return updateByUserBefore[user];}
        std::vector<Event> getUpdatesAfter(string& user){return updateByUserAfter[user];}

        bool getIsDone(string &user){return isDone[user];}
        bool getIsBeforeHalftime(string &user){return halfTimeFalgs[user];}
       
        /*
        A method that enables adding event to a certain user eventlist in the game
        sort after each insertion to maintain time order.
        */
        void addUpdate(string& user,Event& e)
        {
            auto tmp = e.get_game_updates();
            auto it = tmp.find("active");
            if (it != tmp.end() && it->second == "false") { // marks that the user sent end game frame
                isDone[user] = true;
            }

            it = tmp.find("before halftime");
		    if (it != tmp.end() && it->second == "false"){ // marks that the user sent past halftime frame
                halfTimeFalgs[user] = true;
            }

            if(!halfTimeFalgs[user])
            {
                auto& v = updateByUserBefore[user];
                v.push_back(e);
                std::sort(v.begin(), v.end(),[](const Event& a, const Event& b){ return a.get_time() < b.get_time(); }); //sorts vector by time
            }
            else
            {
                auto& v = updateByUserAfter[user];
                v.push_back(e);
                std::sort(v.begin(), v.end(),[](const Event& a, const Event& b){ return a.get_time() < b.get_time(); }); //sorts vector by time
            }
        }
};