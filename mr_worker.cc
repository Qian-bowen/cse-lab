#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
vector<KeyVal> Map(const string &filename, const string &content)
{
	// Copy your code from mr_sequential.cc here.

}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.

}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const vector<string> &filenames);
	void doReduce(int index);
	void doSubmit(mr_tasktype taskType, int index);

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}

void Worker::doMap(int index, const vector<string> &filenames)
{
	// Lab2: Your code goes here.

}

void Worker::doReduce(int index)
{
	// Lab2: Your code goes here.

}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
}

void Worker::doWork()
{
	for (;;) {

		//
		// Lab2: Your code goes here.
		// Hints: send asktask RPC call to coordinator
		// if mr_tasktype::MAP, then doMap and doSubmit
		// if mr_tasktype::REDUCE, then doReduce and doSubmit
		// if mr_tasktype::NONE, meaning currently no work is needed, then sleep
		//

	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

