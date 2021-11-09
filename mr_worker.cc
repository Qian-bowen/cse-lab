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
#include <unordered_set>
#include <algorithm>
#include <regex>

#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
};

void kv2string(vector<KeyVal>& kv,string& str)
{
	for(auto& item:kv)
	{
		str+=item.key+" "+item.val+" ";
	}
}

void string2kv(vector<KeyVal>& kv,string& str)
{
	istringstream iss(str);
	while(!iss.eof())
	{
		string k;
		int v;
		iss>>k;
		iss>>v;
		if(k==""||k==" ") break;
		KeyVal item;
		item.key=k;
		item.val=to_string(v);
		kv.push_back(item);
	}
}


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
	vector<KeyVal> kv;
    regex word_regex("([a-zA-Z]+)");
    auto words_begin=sregex_iterator(content.begin(),content.end(),word_regex);
    auto words_end=sregex_iterator();
    for(auto i=words_begin;i!=words_end;++i)
    {
        smatch match=*i;
        string key=match.str();
        KeyVal tmp;
        tmp.key=key;
        tmp.val="1";
        kv.push_back(tmp);
    }
    return kv;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string Reduce(const string &key, const vector <string> &values)
{
    // Copy your code from mr_sequential.cc here.
    int result=0;
    for(auto& value:values)
    {
        result+=stoi(value);
    }
    return to_string(result);
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int task_num,const string& filename);
	void doReduce(int task_num,int partition);
	void doSubmit(mr_tasktype taskType, int index);
	void partitionAndAppend(int task_num,vector<KeyVal>& kv);
	bool readFile(const string filename,string& content);
	bool writeFile(const string filename,const string& content);

	inline string interMediateName(int task_num,int partition)
	{
		return "mr-"+to_string(task_num)+"-"+to_string(partition);
	}

	inline string outName(int task_num,int partition)
	{
		return "mr-out-"+to_string(task_num)+"-"+to_string(partition);
	}

	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
	MAPF mapf;
	REDUCEF reducef;

	int PARTITION_R=REDUCER_COUNT;//partition number
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

//?based on own filesystem
bool Worker::readFile(const string filename,string& content)
{
	ifstream file(filename);
	stringstream buffer;
	buffer << file.rdbuf();
	content=buffer.str();

	file.close();
	return true;
}

bool Worker::writeFile(const string filename,const string& content)
{
	//append file
	ofstream file(filename, ofstream::out|ios_base::app);
	if(!file.is_open())
	{
		return false;
	}
	file<<content;
	file.close();
	return true;
}

void Worker::partitionAndAppend(int task_num,vector<KeyVal>& kv)
{
	vector<vector<KeyVal>> kvv(PARTITION_R,vector<KeyVal>());
	for(auto& item:kv)
	{
		size_t hash_key=hash<string>{}(item.key);
		kvv[hash_key%PARTITION_R].push_back(item);
	}
	//write into files
	int cur_partition=0;
	for(auto& kv:kvv)
	{
		if(kv.empty()) continue;
		string content;
		kv2string(kv,content);
		writeFile(interMediateName(task_num,cur_partition),content);
		++cur_partition;
	}
}

void Worker::doMap(int task_num,const string& filename)
{
	// Lab2: Your code goes here.
	string content;
	if(!readFile(filename,content))
	{
		return;
	}
	vector<KeyVal> kv=Map(filename,content);
	//partition and write locally
	partitionAndAppend(task_num,kv);
}

void Worker::doReduce(int task_num,int partition)
{
	// Lab2: Your code goes here.

	// in real system, reduce should read from all machines
	// but in this lab just from one
	string content;
	vector<KeyVal> intermediate;
	if(!readFile(interMediateName(task_num,partition),content))
	{
		return;
	}
	string2kv(intermediate,content);
	//sort keyvalue
	sort(intermediate.begin(), intermediate.end(),
    	[](KeyVal const & a, KeyVal const & b) {
		return a.key < b.key;
	});

	string out_str="";
	for (unsigned int i = 0; i < intermediate.size();) {
        unsigned int j = i + 1;
        for (; j < intermediate.size() && intermediate[j].key == intermediate[i].key;)
            j++;

        vector<string> values;
        for (unsigned int k = i; k < j; k++) {
            values.push_back(intermediate[k].val);
        }

        string output = Reduce(intermediate[i].key, values);
		out_str+=intermediate[i].key+" "+output+'\n';
        i = j;
    }
	writeFile(outName(task_num,partition),out_str);
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
		int empty;
		mr_protocol::AskTaskResponse reply;
		mr_protocol::status ret=this->cl->call(mr_protocol::asktask,empty,reply);
		if(reply.task_type==mr_tasktype::MAP)
		{
			doMap(reply.task_num,reply.filename);
			doSubmit(mr_tasktype::MAP,reply.index);
		}
		else if(reply.task_type==mr_tasktype::REDUCE)
		{
			doReduce(reply.task_num,reply.index);
			doSubmit(mr_tasktype::REDUCE,reply.index);
		}
		else
		{
			//todo: sleep for how long?
			sleep(1);
		}

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

