#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void recovery(int& current_term,int& voted_for,std::vector<log_entry<command>>& recovery_log);
    void persist_current_term(int current_term);
    void persist_voted_for(int voted_for);
    void persist_entry(log_entry<command> entry);

private:
    std::mutex mtx;
    std::string dir_name;
    std::string filename;
    std::string meta_filename;

private:
    bool is_empty(std::ifstream& file);
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    dir_name=dir;
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
}

/**
 * @brief 
 * meta-file(binary):current_term(int),voted_for(int)
 * data-file(binary):log(term cmd)
 * not store empty log
 * @tparam command 
 * @param myid 
 * @param cmd_size 
 */
template<typename command>
void raft_storage<command>::recovery(int& current_term,int& voted_for,std::vector<log_entry<command>>& recovery_log)
{
    filename=dir_name + "data" ;
    meta_filename=dir_name + "meta";
    std::ifstream data(filename,std::ios::binary), meta(meta_filename,std::ios::binary);

    // new file
    if(is_empty(date)&&is_empty(meta))
    {
        meta<<(int)0;
        meta<<(int)(-1);
    }
    else if(!is_empty(date) && !is_empty(meta))
    {
        meta>>current_term;
        meta>>voted_for;
        while(!data.eof())
        {
            int cur_term;
            command cur_cmd;
            int cmd_size=cur_cmd.size();
            char* buf=new char[cmd_size];
            data>>cur_term;

            data.read(buf,cmd_size);
            cur_cmd.deserialize(buf,cmd_size);
            delete buf;

            recovery_log.push_back(command(cur_term,cur_cmd));
        }
    }
    else
    {
        meta.close();
        data.close();
        assert(0);
    }

    meta.close();
    data.close();
}

template<typename command>
void raft_storage<command>::persist_current_term(int current_term)
{
    std::unique_lock<std::mutex> lock(mtx);
    std::ifstream meta(meta_filename,std::ios::binary);
    meta.seekg(0, ios::beg);
    meta<<current_term;
    meta.close();
}

template<typename command>
void raft_storage<command>::persist_voted_for(int voted_for)
{
    std::unique_lock<std::mutex> lock(mtx);
    std::ifstream meta(meta_filename,std::ios::binary);
    meta.seekg(sizeof(int), ios::beg);
    meta<<voted_for;
    meta.close();
}

template<typename command>
void raft_storage<command>::persist_entry(log_entry<command> entry)
{
    std::unique_lock<std::mutex> lock(mtx);
    std::ifstream data(filename,std::ios::binary|std::ios::app);
    data<<entry.term;
    command cmd=entry.cmd;
    int cmd_size=cmd.size();
    char* buf=new char[cmd_size];
    cmd.serialize(buf,cmd_size);
    data.write(buf,cmd_size);
    delete buf;
    data.close();
}

template<typename command>
bool raft_storage<command>::is_empty(std::ifstream& file)
{
    file.seekg(0,ios::end);
    return file.tellg()==0;
}

#endif // raft_storage_h