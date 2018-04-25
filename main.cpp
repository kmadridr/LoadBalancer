/*
Un gran poder conlleva una gran responsabilidad
-C++
*/

/*
  Balanceador creado por el Drim Team
  La idea de este balanceador es establecer la comunicacion con los PI y estar escuchando y escribiendo en todo momento.


*/
#pragma region include
#include <iostream>
#include <string>
#include <sstream>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <map>
#include <netdb.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <fstream>
#include <thread>
#include <mutex>
#include <vector>
#include <sstream>
#pragma endregion include

using namespace std;

#define se_libre detach
#define WORLD_EXISTS 1

typedef struct
{
  int jobId;
	int clientNode;
	int assignedWorker;
	string directory;
} Job;

vector<int> file_descriptors;
map<int, string> workers; //numero de nodo, ip
mutex workers_mutex;

int nodoAsigned = 0; //
mutex roundRobin_mtx;

map<int, Job> jobs;
mutex job_mtx;
int jobcounter;

void writer();
void listener(int,int,int, string);
int balancer(vector<string>, int);
void sendJob(int);
void killJob(int);
int checkJob(int);
void finishJob(int);
void executeJob(vector<string>, int);
void sendStatus(int, int, int);
void reassignWorkers(int);

int roundRobin(){
	roundRobin_mtx.lock();
	int nodo;
	do {
    nodoAsigned = (nodoAsigned + 1) % file_descriptors.size();
		nodo = nodoAsigned;
	} while (workers.count(nodo) == 0);
	roundRobin_mtx.unlock();
  return nodo;
}

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
    return elems;
}

int main(int argc, char *argv[])
{
    int port = 8080;

    // Buffer en donde el Balanceador va guardar la informacion que va enviar
    char msg[1500];

    map<int, thread> nodos;

    // Estructura del servidor. Contiene protocolo y todo
    sockaddr_in servAddr; // La crea
    bzero((char*)&servAddr, sizeof(servAddr)); // La limpia
    servAddr.sin_family = AF_INET; // Asigna IP4
    servAddr.sin_addr.s_addr = htonl(INADDR_ANY); //Especifica addres
    servAddr.sin_port = htons(port); //Asigna puerto

    int serverSd = socket(AF_INET, SOCK_STREAM, 0); // Crea el archivo que se va estar utulizando para ocmunicar y Especifica con segundo argumtn o TCP protool
    if(serverSd < 0)
    {
        cerr << "Error establishing the server socket" << endl;
        exit(0);
    }

    // Obliga a hacer la conexion! La hace mucho de pedo para reconectar si no tiene esto
    int opt = 1;
    if( setsockopt(serverSd, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 ){
      cerr << "Error" << endl;
      exit(0);
    }

    // Permite la comunicacion SUDO para poder hacer esto
    int bindStatus = bind(serverSd, (struct sockaddr*) &servAddr,
        sizeof(servAddr));
    if(bindStatus < 0){
        cerr << "Error binding socket to local address" << endl;
        exit(0);
    }

    cout << "Waiting for clients to connect..." << endl;
    //Puede conectar 2 a la vez.
    listen( serverSd, 2 );

    // Estructura para la nueva conexion
    sockaddr_in newSockAddr;
    socklen_t newSockAddrSize = sizeof( newSockAddr );

    // thread t( writer ); //Thread para escribir
    // t.se_libre(); // Se libre amigo

    while(WORLD_EXISTS){
      // Nueva conexion
      int newSd = accept(serverSd, (sockaddr *)&newSockAddr, &newSockAddrSize);
      if(newSd < 0){
        cerr << "Error accepting request from client!" << endl;
        exit(1);
      }
      char *ip = inet_ntoa(newSockAddr.sin_addr);
      string s_ip = ip;
      cout << "Conectado al IP:" << ip << endl;
      nodos[nodos.size()] = thread(listener, serverSd, newSd, nodos.size(), s_ip); // Alberca de nodos RAPS
      file_descriptors.push_back(newSd);
      cout<< "Listener thread created" << endl;
    }

  return 0;

}

void writer(){

  char msg[1500];

  while(WORLD_EXISTS){ // While energy supply exists
    // cout << ">";
    string data; // We need string tho if is bigger than buffersito el resto sera ignorado gracias al buen metodo str n cpy which takes the tamano of the buffer in consideration
    getline(cin, data);

    vector<string> t = split(data, ':');

    memset(&msg, 0, sizeof(msg));//clear the buffer

    strncpy(msg, t.at(1).c_str(),sizeof(msg)); // Convierte string a characteres y lo pega en mi buffer

    int nodeNumber = atoi(t.at(0).c_str()); // Numero de nodo siendo usado

    send(file_descriptors.at( nodeNumber ), (char*)&msg, strlen(msg), 0); // No pos hace mushas cosas. Bandera 0 important para blocking which we do need

    memset(&msg, 0, sizeof(msg));//clear the buffer place 0 from buffer to offset

  }
}

void listener(int serverSd, int newSd, int numNodo, string ip){
  vector<thread> threads;
  // Buffer
  char msg[1500];
  int bytesRead;

  while( 1 ){
    memset(&msg, 0, sizeof(msg)); // Clear the buffershito
    bytesRead = recv(newSd, (char*)&msg, sizeof(msg), 0);
    if(!strcmp( msg, "exit" )){
      cout << "Nodo desconectado voluntariamente." << endl;
      //Call balance
      break;
    }
		if(bytesRead <= 0){
      cout << "Nodo desconectado involuntariamente." << endl;
      //Call balance
      break;
    }

    if (!strcmp(msg, "worker")) { //El primer mensaje de cada worker es este, para identificarlos
      workers_mutex.lock();
      workers.insert(pair <int, string>(numNodo, ip));
      workers_mutex.unlock();
      cout << "Identificado como worker!" << endl;
    }
    string input = msg;
    vector<string> i;
    i = split(input, ' ');

    // //Si es submit (solo desde clientes)
    // cout << "Entrada: " << i.at(0).c_str() << endl;

    if ( i.at(0).compare( "submit" ) == 0 ) {
      cout << "Job received" << endl;
      thread t(&executeJob, i, numNodo);
      t.detach();
    }

    //Matar un trabajo (desde clientes)
    else if ( i.at(0).compare( "kill" ) == 0 ) {
      cout << "Kill signal received" << endl;
      thread t (&killJob, stoi(i.at(1)));
      t.detach();
      // killJob(stoi(i.at(1)));
    }
    //Revisar status de un trabajo
    else if ( i.at(0).compare( "check" ) == 0 ) {
      cout << "Check status" << endl;
      int worker = checkJob(stoi(i.at(1)));
      thread t (&sendStatus, worker, stoi(i.at(1)), numNodo);
      t.detach();
      // checkJob(stoi(i.at(1)));
    }
    //Enviado por worker al finalizar la ejecucion de un job
    //Para este entonces, el output ya fue copiado del worker al balancer
    else if ( i.at(0).compare( "finish" ) == 0 ) {
      cout << "Job " << stoi(i.at(1)) << " finished" << endl;
      thread t (&finishJob, stoi(i.at(1)));
      t.detach();
      // finishJob(stoi(i.at(1)));
    }
  }
  // close(newSd);
  // close(serverSd);
  workers_mutex.lock();
  workers.erase(numNodo);
  workers_mutex.unlock();
  reassignWorkers(numNodo);
}



void executeJob(vector<string> i, int numNodo)
{
  int newJobId = balancer(i, numNodo);
  //Enviar confirmacion al cliente
  //algo como: send ("el id de tu trabajo es " + jobid)
  char msg[1500];
  string message = "El id del trabajo es " + to_string(newJobId);
  strncpy(msg, message.c_str() ,sizeof(msg));
  send(file_descriptors.at( jobs.at(newJobId).clientNode ), (char*)&msg, message.length(), 0);
  sendJob(newJobId);
}

int balancer(vector<string> i, int numNodo)
{
  //Logica para el balanceador
  //Seleccionar worker (Reemplazar por una funcion)
	  int workerId = 0; //DEFAULT
    workerId = roundRobin(); //Regresa nodo a usarse
    cout << "Job assigned to node " << workerId << endl;
    //Crear el job
	  job_mtx.lock();
	  Job j;
	  int jobid = jobcounter++;
    j.jobId = jobid;
	  j.clientNode = numNodo;
	  j.assignedWorker = workerId;
	  j.directory = i.at(1);
	  jobs.insert(pair<int, Job>(jobid, j));
	  job_mtx.unlock();

    return jobid;
}

void sendJob(int jobId)
{
  // cout << "SendJob " << jobId << endl;
  // cout << jobs.count(jobId) << endl;
  // cout << jobs.at(jobId).assignedWorker << endl;
  //Logica para enviar un trabajo recibido a un nodo
  int workerId = jobs.at(jobId).assignedWorker;
  string directory = jobs.at(jobId).directory;
  //Crear el directorio en el nodo asignado
    workers_mutex.lock();
    if(workers.count(workerId) == 0) {
      workers_mutex.unlock();
      return;}
	  string cmd = "ssh pi@" + workers.at(workerId) + " 'mkdir -p //home/pi/loadbalancer/jobs/" + directory +"'"; //Verificar comando
	  cout << cmd << endl;
    workers_mutex.unlock();
	  system(cmd.c_str());

    workers_mutex.lock();
    if(workers.count(workerId) == 0) {
      workers_mutex.unlock();
      return;}
	  //Copiar carpeta
	  cmd = "scp -r //home/ludanortmun/repos/loadbalancer/jobs/" + directory + "/* pi@" + workers.at(workerId) + ":loadbalancer/jobs/" + directory; //verificar comando
	  cout << cmd << endl;
    workers_mutex.unlock();
	  system(cmd.c_str());

	  //Enviar mensaje al nodo para que inicie el job
	  //sendMessage(workerId, ("submit " + i.at(1) + " " + jobid));
    char msg[1500];
    string message = "submit " + directory + " " + to_string(jobId);
    strncpy(msg, message.c_str() ,sizeof(msg));
    workers_mutex.lock();
    if(workers.count(workerId) == 0) {
      workers_mutex.unlock();
      return;}
    workers_mutex.unlock();
    send(file_descriptors.at( workerId ), (char*)&msg, message.length(), 0);
}

void killJob(int jobId)
{
  //Logica para terminar un trabajo
  job_mtx.lock();
  if (jobs.count(jobId) != 0) {
    char msg[1500];
    string message = "kill " + to_string(jobId);
    strncpy(msg, message.c_str() ,sizeof(msg));
    send(file_descriptors.at( jobs.at(jobId).assignedWorker ), (char*)&msg, message.length(), 0);
    string cmd = "rm -r //home/ludanortmun/repos/loadbalancer/jobs/" + jobs.at(jobId).directory;
    system(cmd.c_str());
    jobs.erase(jobId);
  }
  job_mtx.unlock();
}

int checkJob(int jobId)
{
  //Logica para revisar el estado de un trabajo
  int worker;
  if (jobs.count(jobId) == 0) { //No existe en la lista
    worker =  -1;
  }
  else { //Regresar el worker asignado
    worker = jobs.at(jobId).assignedWorker;
  }
  return worker;
}

void sendStatus(int worker, int jobId, int clientNode)
{
  //status {jobId} {worker}
  string message = "status " + to_string(jobId) + " " + to_string(worker);
  char msg[1500];
  strncpy(msg, message.c_str() ,sizeof(msg));
  send(file_descriptors.at( clientNode ), (char*)&msg, message.length(), 0);
}

void finishJob(int jobId)
{
  if(jobs.count(jobId) != 0){ //Validacion en caso de que se envio un kill signal antes de ejecucion
    //Logica para enviar el resultado al cliente
    //Transferir los contenidos del nodo al balanceador
    workers_mutex.lock();
    job_mtx.lock();
    string dir = jobs.at(jobId).directory;
    int workerId = jobs.at(jobId).assignedWorker;
    job_mtx.unlock();
    if(workers.count(workerId) == 0) { 
      workers_mutex.unlock();
      return; }
    string cmd = "scp -r pi@" + workers.at(workerId) + ":loadbalancer/jobs/" + dir + "/* //home/ludanortmun/repos/loadbalancer/jobs/" + dir + "/";
    cout << cmd << endl;
    workers_mutex.unlock();
    system(cmd.c_str());

    //Eliminar el contenido del nodo
    workers_mutex.lock();
    if(workers.count(workerId) == 0 ) {
      workers_mutex.unlock();
      return;}
    cmd = "ssh pi@" + workers.at(workerId) + " 'rm -r //home/pi/loadbalancer/jobs/" + dir + "'";
    cout << cmd << endl;
    workers_mutex.unlock();
    system(cmd.c_str());

    //Enviar confirmacion al cliente
    char msg[1500];
    string message = "finish " + dir + " " + to_string(jobId);
    strncpy(msg, message.c_str() ,sizeof(msg));
    job_mtx.lock();
    if(jobs.count(jobId) == 0) {
      job_mtx.unlock();
      return;}
    send(file_descriptors.at( jobs.at(jobId).clientNode ), (char*)&msg, message.length(), 0);
    job_mtx.unlock();

    //Eliminar de la lista de trabajos
    job_mtx.lock();
    if(jobs.count(jobId) != 0) //En caso de que se haya enviado kill antes de esto
    {
      jobs.erase(jobId);
    }
    job_mtx.unlock();
  }
}

void reassignWorkers(int numNodo) 
{
  vector<int> temp;
  job_mtx.lock();
  for(pair<const int, Job>& job_pair : jobs)
  {
    Job& j = job_pair.second;
    if(j.assignedWorker == numNodo){
      j.assignedWorker = roundRobin();
      cout << "Job reassigned to node " << j.assignedWorker << endl;
      temp.push_back(job_pair.first);
    }
  }
  job_mtx.unlock();
  for(auto& j : temp){
    thread t(sendJob, j);
    t.detach();
  }
}