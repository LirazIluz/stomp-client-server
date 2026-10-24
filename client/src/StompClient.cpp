#pragma once
#include <iostream>
#include <stdio.h>
#include "../include/ConnectionHandler.h"
#include "../include/StompProtocol.h"
#include "../include/event.h"
#include "../include/Game.h"
#include <atomic>
#include <mutex>
#include <thread>
#include <map>
#include <vector>
#include <stdexcept>
#include <fstream>

#define COMMAND 0
#define ACTION 0
#define HOST_ADDRESS 1
#define TOPIC 1
#define FILE_PATH 1
#define USERNAME 2
#define PASSWORD 3
#define SUMMARY_FILE_INDEX 3

#define BODY_START 6
#define USER_LINE 5
#define DEST_LINE 3

using std::string;
using std::cout;
using std::endl;

std::mutex receiptMutex;
std::mutex updateMutex;

std::mutex connectionMutex;
std::condition_variable cv; // for wait notify logic

ConnectionHandler * connect(string& host,string& port);
void addReceipt(int rec,string action,std::map<int,string>& receipts);
string removeReceipt(int rec,std::map<int,string>& receipts);
void threadLoop(StompProtocol& protocol,std::map<int,string>& receipts);
std::vector<string> parseCommand(string& input,int len);
std::vector<string> split(string input,char splitBy);
std::vector<string> getLines(string& str);
void process(std::map<int, std::string>& reciepts,std::map<string,int>& subscriptions,std::vector<string>& command,
StompProtocol& protocol,int receiptIdGen,int subIdGen);


int main(int argc, char *argv[]) {
	
	ConnectionHandler * con = nullptr;
	const short bufsize = 1024;
    char buf[bufsize];
	int receiptIdGen = 0;
	int subIdGen = 0;

	std::map<int, std::string> reciepts;
	std::map<string,int> subscriptions;

	StompProtocol protocol;
	
	std::thread t(threadLoop,std::ref(protocol),std::ref(reciepts)); //start the thread that listens to server
	
	cout<< "enter command" << endl;
	while(!protocol.shouldTerminate()) // runs until should terminate
	{
		try
		{
			std::cin.getline(buf, bufsize);
			std::string line(buf);
			int len=line.length();

			std::vector<string> command = parseCommand(line,len); // parse command
			string comm= command[COMMAND];
			if(comm=="login") //login command
			{
				std::vector<string> hostInfo = split(command[HOST_ADDRESS],':');
				if(con != nullptr)
				{
					con->close(); // wakers waiting thread
					protocol.setIsConnected(false);
					delete con;
					con = nullptr;
				}
				reciepts.clear();
				subscriptions.clear();// clearing maps on new connection.
				con = connect(hostInfo[0],hostInfo[1]); // connecting
				protocol.start(con,command[USERNAME]);

				protocol.handleLogin(command[HOST_ADDRESS],command[USERNAME],command[PASSWORD]); // sends login
			}
			else
			{
				if(!protocol.getIsConnected())
					cout << "You first must login to host before using any commands other than login." << endl;
				else
				{
					process(reciepts,subscriptions,command,protocol,receiptIdGen,subIdGen);
				}
			}
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
	}

	t.join();
	return 0;
}
/*
Method that responsible connecting connection handler and returns a pointer to it. - no defult constructor that it was handeled this way.
*/
ConnectionHandler * connect(string& host,string& port) {

	cout << "Connecting to " + host + " on port: " + port << endl;
	ConnectionHandler *ch = new ConnectionHandler(host, std::stoi(port));

    if (!ch->connect()) {
        std::cerr << "Cannot connect to " << host << ":" << port << std::endl;
		delete ch;
        throw std::runtime_error("Cannot connect to " + host + ":" + port);
    }
	
	return ch;
}

void process(std::map<int, std::string>& reciepts,std::map<string,int>& subscriptions,std::vector<string>& command,
StompProtocol& protocol,int receiptIdGen,int subIdGen)
{
	if(command[COMMAND] =="join")
	{
		if(subscriptions.find(command[TOPIC])!= subscriptions.end()) //locates in subscription map first, send frame if not subscribed
			cout<< "User already subscribed to the topic." << endl;
		else
		{
					
			subscriptions[command[TOPIC]] = subIdGen; // saves subscription in map
			addReceipt(receiptIdGen,"SUB:" + command[TOPIC],reciepts); //put a receipt in the receipt map in the format id->action:topic
			protocol.handleJoin(command[TOPIC],subIdGen,receiptIdGen);
			subIdGen++;
			receiptIdGen++;
		}
	}
	else if(command[COMMAND] == "exit") //exit command
	{
		if(subscriptions.find(command[TOPIC])== subscriptions.end())
			cout<< "You are subscribed to that topic." << endl;
		else
		{
			addReceipt(receiptIdGen,"UNSUB:" + command[TOPIC],reciepts);
			protocol.handleExit(command[TOPIC],subscriptions[command[TOPIC]],receiptIdGen);
			subscriptions.erase(command[TOPIC]);
			receiptIdGen++;
		}
	}
	else if(command[COMMAND] == "report")
	{
		protocol.handleReport(command[FILE_PATH]);
		cout << "Report was sent." << endl;
	}
	else if(command[COMMAND] == "logout")
	{
		addReceipt(receiptIdGen,"LOGOUT:LOGOUT",reciepts);
		protocol.handleLogout(receiptIdGen);
		receiptIdGen++;
	}
	else if(command[COMMAND] == "summary")
	{
		protocol.handleSummary(command[TOPIC],command[USERNAME],command[SUMMARY_FILE_INDEX]);
		cout << "A summary was created at:" + command[SUMMARY_FILE_INDEX] << endl;
	}
	else
	{
		cout << "Unknown command: " + command[COMMAND] +" try again." << endl;
	}
}

/*
Adds a receipt to the common map, locking the mutex and frees it.
*/
void addReceipt(int rec,string action,std::map<int,string>& receipts)
{
	std::lock_guard<std::mutex> lock(receiptMutex);
	receipts[rec] = action;
}
/*
Removes a receipt from common map and returns it, locking the mutex and frees it.
*/
string removeReceipt(int rec,std::map<int,string>& receipts)
{
	std::lock_guard<std::mutex> lock(receiptMutex);
	string res = receipts[rec];
	receipts.erase(rec);

	return res;
}
/*
The function that the thread thats listening to server runs.
active until connection should terminate
*/
void threadLoop(StompProtocol& protocol,std::map<int,string>& receipts)
{
	while(!protocol.shouldTerminate())
	{
		try
		{
			std::unique_lock<std::mutex> lock(connectionMutex);
			while(!protocol.getIsConnected())
				cv.wait(lock);

			lock.unlock();
			protocol.parseResponse(receipts); // reads and act according to frame
		}
		catch(const std::exception& e) // if there was an error terminate the thread
		{
			cout << e.what() << endl;
			protocol.setIsConnected(false);
		}
	}
}
/*
Helper method that splits inial command by spaces, returns a vector of the command parameters.
*/
std::vector<string> parseCommand(string& input,int len)
{
	std::vector<string> res;
	string word ="";
	for(int i=0;i<len;i++)
	{
		if(input[i] == ' ')
		{
			res.push_back(word);
			word.clear();
		}
		else
		{
			word+=input[i];
		}
	}

	res.push_back(word);
	return res;
}

/*
Method that splits a string by input char , assumes it exist in the string.
*/
std::vector<string> split(string input,char splitBy)
{
	std::vector<std::string> res;
    std::string cur = "";

    for (int i = 0; i < input.size(); i++) {
        if (input[i] == splitBy) {
            res.push_back(cur);
            cur = "";
        } else {
            cur += input[i];
        }
    }
    res.push_back(cur);
    return res;
}

/*
Helper method that gets a string and returns a vector with it lines.
*/
std::vector<string> getLines(string& str)
{
	string tmp ="";
	std::vector<string> lines;
	for(int i=0;i<str.size();i++)
	{
		if(str[i] == '\n')
		{
			lines.push_back(tmp);
			tmp.clear();
		}
		else
			tmp+=str[i];
	}
	lines.push_back(tmp); // adding the last line
	return lines;
}

/*
=================================

Stomp protocol implemintations

=================================
*/
StompProtocol::StompProtocol():ch(nullptr),_username(""),shouldTerminateField(false),isConnected(false)
{}

StompProtocol::~StompProtocol()
{
	this->ch->close(); //terminating
	delete this->ch; // deletes allocated ch
}
/*
Method to set is connected to the given status.
*/
void StompProtocol::setIsConnected(bool status)
{
	std::lock_guard<std::mutex> lock(connectionMutex);
	this->isConnected = status;

	cv.notify_all(); //wakes waiting thread
}

/*
Method that iniates protocol.
*/
void StompProtocol::start(ConnectionHandler * ch, string username)
{
	this->ch = ch;
	this->_username = username;
	this->setIsConnected(true);
}

/*
getters for isConnected
*/
bool StompProtocol::getIsConnected()
{
	return this->isConnected;
}

/*
Method that handles login command.construct the frames from given parameters.(CONNECT)
*/
void StompProtocol::handleLogin(string& hostInfo,string& username,string &password)
{
	string host = "";
    int i = 0;

    while (hostInfo[i] != ':') {
        host += hostInfo[i];
        i++;
    } //finding host id from a string formmated 0.0.0.0:port
	string frame = "CONNECT\naccept-version:1.2\nhost:"+host +"\nlogin:" + username +"\npasscode:" + password + "\n\n";
	if (this->shouldTerminate() || !this->ch->sendFrameAscii(frame, '\0')) 
        throw std::runtime_error("A frame could not be sent to the server - shuting down");
    			
        
}

/*
Method that handles join command.construct the frames from given parameters.(SUBSCRIBE)
*/
void StompProtocol::handleJoin(string& game,int subId,int receiptId)
{
	string frame = "SUBSCRIBE\nid:"+ std::to_string(subId)+ "\ndestination:/" + game + "\nreceipt:"+std::to_string(receiptId)+"\n\n";
	if (this->shouldTerminate() ||!this->ch->sendFrameAscii(frame, '\0'))  
        throw std::runtime_error("A frame could not be sent to the server - shuting down");
	this->Games[game] = Game(game);
}

/*
Method that handles exit command.construct the frames from given parameters.(UNSUBSCRIBE)
*/
void StompProtocol::handleExit(string& game,int subId,int receiptId)
{
	string frame = "UNSUBSCRIBE\nid:" + std::to_string(subId) + "\nreceipt:" + std::to_string(receiptId) +"\n\n";
	if (this->shouldTerminate() ||!this->ch->sendFrameAscii(frame, '\0'))  
        throw std::runtime_error("A frame could not be sent to the server - shuting down");

	this->Games.erase(game);
}

/*
Method that handles logout command.construct the frames from given parameters.
*/
void StompProtocol::handleLogout(int receiptId)
{
	string frame = "DISCONNECT\nreceipt:" + std::to_string(receiptId) +"\n\n";
	if (this->shouldTerminate() ||!this->ch->sendFrameAscii(frame, '\0'))  
        throw std::runtime_error("A frame could not be sent to the server - shuting down");
}

/*
Method that handles the report command, construct the frame from parsed events and sending it, also storing the data.
*/
void StompProtocol::handleReport(string& filePath)
{
	names_and_events data = parseEventsFile(filePath); // reading the file
	std::vector<Event> events = data.events;
	string game =  data.team_a_name + "_" +data.team_b_name;
	for(int i=0;i<events.size();i++)
	{
		Event curr = events[i];
		string body = ConstructEventFrame(curr); // consruct body for the frame
		string frame = "SEND\ndestination:/" + game;
		if(i == 0) // first frame sends the file name to log
			frame+="\nfile-name:"+filePath;
		frame += +"\n\n" + body;
		if (this->shouldTerminate() ||!this->ch->sendFrameAscii(frame, '\0'))  //sends frame
        	throw std::runtime_error("A frame could not be sent to the server - shuting down");

		std::map<string, string> tmp = curr.get_game_updates();
		addUpdate(game,_username,curr); // stores data in protocol
		
	}
}

/*
Method that responsible pulling the data from the protocol and printing it to the target file.
*/
void StompProtocol::handleSummary(string& game,string& user,string& filePath)
{
	std::ofstream fileStream(filePath); // opening the file
	if(!fileStream)
		throw std::runtime_error("Cannot open traget file, closing...");

	std::vector<Event> beforeHalf = this->Games[game].getUpdatesBefore(user);
	std::vector<Event> afterHalf = this->Games[game].getUpdatesAfter(user);
	std::vector<string> teams = split(game,'_');
	fileStream << teams[0] +" vs " + teams[1] + "\n";
	fileStream << "Game stats:\n";
	fileStream << "General stats:\n";

	string isActive = (!this->Games[game].getIsDone(user)) ? "true" : "false";
	string isBeforeHalf = (!this->Games[game].getIsBeforeHalftime(user)) ? "true" : "false";
	
	fileStream << "active: "+ isActive +"\n";
	fileStream << "before halftime: "+ isBeforeHalf +"\n";
		
	fileStream << teams[0] + " stats:\n";
	string pos;
	string goals;

	for(Event e:beforeHalf)
	{
		std::map<string,string> tmp =e.get_team_a_updates();
		if(tmp.find("goals")!= tmp.end())
			goals = tmp["goals"];
		if(tmp.find("possession")!= tmp.end())
			pos = tmp["possession"];
	}
	for(Event e:afterHalf)
	{
		std::map<string,string> tmp =e.get_team_a_updates();
		if(tmp.find("goals")!= tmp.end())
			goals = tmp["goals"];
		if(tmp.find("possession")!= tmp.end())
			pos = tmp["possession"];
	}
	fileStream << "goals: " + goals +"\n";
	fileStream << "possession: " + pos +"\n";
	
	pos.clear();
	goals.clear();
	fileStream <<teams[1] + " stats:\n";
	for(Event e:beforeHalf)
	{
		std::map<string,string> tmp =e.get_team_b_updates();
		if(tmp.find("goals")!= tmp.end())
			goals = tmp["goals"];
		if(tmp.find("possession")!= tmp.end())
			pos = tmp["possession"];
	}
	for(Event e:afterHalf)
	{
		std::map<string,string> tmp =e.get_team_b_updates();
		if(tmp.find("goals")!= tmp.end())
			goals = tmp["goals"];
		if(tmp.find("possession")!= tmp.end())
			pos = tmp["possession"];
	}
	fileStream << "goals: " + goals +"\n";
	fileStream << "possession: " + pos +"\n";

	fileStream <<"Game event reports:\n";
	for(Event e:beforeHalf)
		fileStream <<std::to_string(e.get_time()) + " - " + e.get_name() +"\n\n" + e.get_discription() +"\n\n\n";
	for(Event e:afterHalf)
		fileStream <<std::to_string(e.get_time()) + " - " + e.get_name() +"\n\n" + e.get_discription() +"\n\n\n";

		//fileStream closes
}
/*
A method that construct the body for the sent frames according to the manual.
*/
string StompProtocol::ConstructEventFrame(Event& e)
{
	std::map<string,string> gameUpdates = e.get_game_updates();
	std::map<string,string> team_aUpdates = e.get_team_a_updates();
	std::map<string,string> team_bUpdates = e.get_team_b_updates();

	string res = "user: " + this->_username +'\n'
	+ "team a: " + e.get_team_a_name() +'\n'
	+ "team b: " + e.get_team_b_name() +'\n'
	+ "event name: " + e.get_name() +'\n'
	+ "time: " + std::to_string(e.get_time()) +'\n'
	+ "general game updates:\n";

	for (auto [key, value] : gameUpdates)
		res+= "    " +key +": " + value +'\n';

	res +="team a updates:\n";
	for (auto [key, value] : team_aUpdates) 
		res+= "    " +key +": " + value +'\n';

	res +="team b updates:\n";
	for (auto [key, value] : team_bUpdates) 
		res+= "    " +key +": " + value +'\n';
	
	res+="description:\n" + e.get_discription() +'\n';

	return res;
}
/*
Returns if the connection should terminate
*/
bool StompProtocol::shouldTerminate()
{
	return this->shouldTerminateField;
}
/*
set the shouldTerminate flag to status
*/
void StompProtocol::setShouldTerminate(bool status)
{
	this->shouldTerminateField = status;
}

/*
Method that responsible for reading frames from server and acting accordingly
note: receipts are inserted to a map in the format : receiptId->ACTION:TOPIC
*/
void StompProtocol::parseResponse(std::map<int,string>& receipts)
{
	string mes;
	if (!this->ch->getFrameAscii(mes,'\0')) { // reads a frane
        cout << "Disconnected. Exiting...\n" << endl;
    	throw std::runtime_error("Connection was closed, login:\n");
    }
	string frameType;
			
	size_t pos = mes.find('\n');
	frameType = mes.substr(0, pos); //get the frame type sent from server
	if(frameType == "ERROR") // resonse for error
	{
		cout << mes << endl;
		throw std::runtime_error("Connection was closed, login:\n");
	}
	else if(frameType == "CONNECTED") // reponse for connected
	{
		cout <<"Login successful" << endl;
	}
	else if(frameType == "RECEIPT") // reponse for a receipt frame - can be response for subscribe ubnsubscribe and logout.
	{
		int i = mes.find("receipt-id:") + 11;
		int j = mes.find('\n', i);
		int receiptNum = std::stoi(mes.substr(i, j - i));
		std::vector<string> action = split(removeReceipt(receiptNum,receipts),':'); // pulling out the reciet from the receipt map

		if(action[ACTION] == "SUB") // if the action was subscribe
			cout << "Joined channel " + action[TOPIC] << endl;
		else if(action[ACTION] == "UNSUB") //unsubscribe receits
			cout << "Exited channel " + action[TOPIC] << endl;
		else // logout receipt
		{
			cout << "Disconnected from the server" << endl;
			throw std::runtime_error("Connection was terimnated.");
		}
	}
	else if(frameType == "MESSAGE") // reponds to a message frame
	{
		std::vector<string> lines = getLines(mes);
		string username=split(lines[USER_LINE],':')[1].substr(1); // gets the sender
		if(username!=this->_username) // we already store events from the client itself when we send them
		{
			string game = split(lines[DEST_LINE],':')[1].substr(1);
			Event e = parseToEvent(lines); //converting a frame to event
			std::map<string, string> tmp = e.get_game_updates(); 
			addUpdate(game,username,e);// adds game to map
		}
	}
}

/*
A method to add new update for a certain user for the game map.
*/
void StompProtocol::addUpdate(string& game,string& user,Event& e)
{
	std::lock_guard<std::mutex> lock(updateMutex); // locks as the games table is a common resource.
	this->Games[game].addUpdate(user,e);	
}

/*
Method that parses message frame to event according to the frame setup mentioned in the instruction manual
*/
Event StompProtocol::parseToEvent(std::vector<string> lines)
{
	std::string teamA, teamB, eventName, desc;
    int time = 0;
    std::map<std::string,std::string> gameUpdates, teamAUpdates, teamBUpdates;

	int i = BODY_START;
	teamA = split(lines[i],':')[1].substr(1);
	i++;
	teamB = split(lines[i],':')[1].substr(1);
	i++;
	eventName = split(lines[i],':')[1].substr(1);
	i++;
	time = std::stoi(split(lines[i],':')[1].substr(1));
	i++;

	i++;//skipping the string general game updates
	std::vector<string> section;
	if(lines[i][0]==' ')
	{
		while(lines[i][0] == ' ')
		{
			section.push_back(lines[i].substr(4)); // removing the tab at the start
			i++;
		}
		for(int j =0;j<section.size();j++)
		{
			std::vector<string> line = split(section[j],':');
			gameUpdates[line[0]] = line[1].substr(1);
		}
	}

	i++; // skipping team a updates
	section.clear();
	if(lines[i][0]==' ')
	{
		while(lines[i][0] == ' ')
		{
			section.push_back(lines[i].substr(4)); // removing the tab at the start
			i++;
		}
		for(int j =0;j<section.size();j++)
		{
			std::vector<string> line = split(section[j],':');
			teamAUpdates[line[0]] = line[1].substr(1);
		}
	}
	
	i++; // skipping team b update
	section.clear();
	if(lines[i][0]==' ')
	{
		while(lines[i][0] == ' ')
		{
			section.push_back(lines[i].substr(4)); // removing the tab at the start
			i++;
		}
		for(int j =0;j<section.size();j++)
		{
			std::vector<string> line = split(section[j],':');
			teamBUpdates[line[0]] = line[1].substr(1);
		}
	}
	
	i++;
	while(i<lines.size())// constructing description
	{
		desc += lines[i];
		i++;
	}

	return Event(teamA,teamB,eventName,time,gameUpdates,teamAUpdates,teamBUpdates,desc);
}
