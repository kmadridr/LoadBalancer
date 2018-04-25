/*
Un gran poder conlleva una gran responsabilidad
-C++
*/
#pragma region include
#include <iostream>
#include <string>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <thread>
#include <sstream>
#include <vector>
#include <fstream>
#include <map>
#include <mutex>
#pragma endregion include

using namespace std;

#define WORLD_EXISTS 1

void listener(int);
void execute(string, int, int);
void killJob(int);

char *serverIp;

map<int, bool> jobs;
mutex job_mtx;

void split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

vector<string> split(const string &s, char delim) {
    vector<string> elems;
    split(s, delim, elems);
    return elems  ;
}

int main(int argc, char *argv[]){
  if(argc != 2){
    cout << "Error: Missing IP." << endl;
    exit(1);
  }
  serverIp = argv[1];
  int port = 8080;
  //create a message buffer
  char msg[1500];

  struct hostent* host = gethostbyname(serverIp);
  sockaddr_in sendSockAddr;
  bzero((char*)&sendSockAddr, sizeof(sendSockAddr));
  sendSockAddr.sin_family = AF_INET; // IPv4
  sendSockAddr.sin_addr.s_addr = inet_addr(inet_ntoa(*(struct in_addr*)*host->h_addr_list));
  sendSockAddr.sin_port = htons(port); //Convierte en binario
  int clientSd = socket(AF_INET, SOCK_STREAM, 0);
    //try to connect...
  int status = connect(clientSd, (sockaddr*) &sendSockAddr, sizeof(sendSockAddr));
  if(status < 0){
    cout<<"Error connecting to socket!" << endl;
  }
  cout << "Connected to the server!" << endl;
  send(clientSd, (char*)("worker"),6,0);
  int bytesRead;

  // thread t(listener, clientSd);
  // t.detach(); // A partir de este momento eres una persona independiente.

  // while(WORLD_EXISTS){
  //   // cout << ">";
  //   string data;
  //   getline(cin, data);
  //   memset(&msg, 0, sizeof(msg));//clear the buffer
  //   strncpy(msg, data.c_str(), sizeof(msg));
  //   if(data == "exit"){
  //     send(clientSd, (char*)&msg, strlen(msg), 0);
  //     break;
  //   }
  //   send(clientSd, (char*)&msg, strlen(msg), 0);
  //       //cout << "Awaiting server response..." << endl;
  //   memset(&msg, 0, sizeof(msg));//clear the buffer
  //   //    bytesRead += recv(clientSd, (char*)&msg, sizeof(msg), 0);
  //   if(!strcmp(msg, "exit")){
  //     cout << "Server has quit the session" << endl;
  //     break;
  //   }
  // }
  listener(clientSd);
  close(clientSd);
  cout << "Connection closed" << endl;
  return 0;
}

void listener(int clientSd){
  cout << "Entre a listener" << endl;
  char msg[1500];
  while(1){
    memset(&msg, 0, sizeof(msg));
    int m = recv(clientSd, (char*)&msg, sizeof(msg), 0);
    if(m > 0){
      string tmp = msg;
      vector<string> input;
      input = split(tmp, ' ');

      if( input.at(0).compare( "submit" ) == 0 ){
        cout << "Job received" << endl;
        string dir = input.at(1);
        thread t(execute, dir, clientSd, stoi(input.at(2)));
        t.detach();
      }
      else if( input.at(0).compare( "kill" ) == 0) {
        //mata proceso
        thread t(killJob, stoi(input.at(1)));
        t.detach();
        // kill(stoi(input.at(1)));
      }
    }
  }
}

void execute(string path, int clientSd, int jobId)
{
  job_mtx.lock();
  jobs.insert(pair<int, bool>(jobId, false));
  job_mtx.unlock();

  //Ejecutar el job
	string command = "cd //home/pi/loadbalancer/jobs/" + path + "; python3 //home/pi/loadbalancer/src/ProgramHandler.py " + "//home/pi/loadbalancer/jobs/" + path + "/job.json";
	cout << command << endl;
  system(command.c_str());

  //Si el trabajo no fue terminado
  if (!jobs.at(jobId)) {
    //Enviar confirmacion al balancer
    char msg[1500];
    string message = "finish " + to_string(jobId);
    strncpy(msg, message.c_str() ,sizeof(msg));
    send(clientSd, (char*)&msg, message.length(), 0);
  }
  job_mtx.lock();
  jobs.erase(jobId);
  job_mtx.unlock();
}

void killJob(int jobId)
{
  //Matar al trabajo
  if (!jobs.count(jobId) == 0) { //Si existe
    job_mtx.lock();
    jobs.at(jobId) = true;
    job_mtx.unlock();
  }
}

