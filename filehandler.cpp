#include <stdlib.h>
#include <iostream>
#include <vector>

using namespace std;

struct node
{
	string ip;
	string user;
};

vector<node> nodes;

class FileHandler{
    public:
        bool copy(int from, string fpath, int to, string tpath){
            string cmd = "scp " + nodes[from].user + "@" + nodes[from].ip +":"+ fpath + " " + nodes[to].user + "@" + nodes[to].ip +":"+ tpath;
            system(cmd.c_str());
            return true;
        }

        bool copy(string from, string to){
            string cmd = "scp " + from + " " + to;
            system(cmd.c_str());
            return true;
        }
       
        void addNode(string ip, string user){
        	node n;
        	n.ip = ip;
        	n.user = user;
        	nodes.push_back(n);
        }

        void addNode(string ip){
        	node n;
        	n.ip = ip;
        	n.user = "pi";
        	nodes.push_back(n);
        }
};

int main(){
	FileHandler fh;
	fh.addNode("127.0.1.1");
	fh.copy(0, "/home", 0, "/home/test");
	return 0;
}