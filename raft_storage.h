#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>

// #define DEBUG

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void recovery(int& current_term,int& voted_for,std::vector<log_entry<command>>& recovery_log);
    void persist_current_term(int current_term);
    void persist_voted_for(int voted_for);
    void persist_log(std::vector<log_entry<command>> log);
    void set_node_info(int my_id_){my_id=my_id_;}//for debug

private:
    std::mutex mtx;
    std::fstream data,meta;
    int my_id;

private:
    bool is_empty(std::fstream& file);
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    std::string filename=dir+"/data" ;
    std::string meta_filename=dir+"/meta";

    data.open(filename,std::fstream::binary|std::fstream::in | std::fstream::out);
    meta.open(meta_filename,std::fstream::binary|std::fstream::in | std::fstream::out);
    if(!data.is_open())
    {
        data=std::fstream(filename,std::fstream::binary|std::fstream::in | std::fstream::out| std::fstream::trunc);
    }
    if(!meta.is_open())
    {
        meta=std::fstream(meta_filename,std::fstream::binary|std::fstream::in | std::fstream::out| std::fstream::trunc);
    }

    // data=std::fstream(filename,std::fstream::binary|std::fstream::in | std::fstream::out| std::fstream::trunc);
    // meta=std::fstream(meta_filename,std::fstream::binary|std::fstream::in | std::fstream::out| std::fstream::trunc);

    assert(data.good());
    assert(data.is_open());
    assert(meta.good());
    assert(meta.is_open());
}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   data.close();
   meta.close();
}

/**
 * @brief 
 * meta-file(binary):current_term(int),voted_for(int),log num(int)
 * data-file(binary):log(term cmd)
 * store empty log
 * log num is add because if lots log are added to file at first, add delete something at last,the file size won't become small
 * @tparam command 
 * @param myid 
 * @param cmd_size 
 */
template<typename command>
void raft_storage<command>::recovery(int& current_term,int& voted_for,std::vector<log_entry<command>>& recovery_log)
{
    #ifdef DEBUG
    std::cout<<"node:"<<my_id<<"recovery start"<<std::endl;
    #endif
    assert(data.good());
    assert(data.is_open());
    // new file
    if(is_empty(data)&&is_empty(meta))
    {
        assert(data.good());
        assert(data.is_open());
        // if(!meta.is_open()){assert(0);}
        #ifdef DEBUG
        std::cout<<"empty start"<<std::endl;
        #endif
        data.seekp(0, std::ios::beg);
        meta.seekp(0, std::ios::beg);

        current_term=0;
        voted_for=-1;
        int log_num=0;

        // if(!meta.good()){assert(0);}

        // persist initialize data
        meta.write(reinterpret_cast<const char *>(&current_term), sizeof(current_term));
        meta.write(reinterpret_cast<const char *>(&voted_for), sizeof(voted_for));
        meta.write(reinterpret_cast<const char *>(&log_num), sizeof(log_num));
    
        meta.flush();

        // if(!meta.good()){assert(0);}

    

        log_entry<command> empty(0);
        command cmd=empty.cmd;
        int cmd_size=cmd.size();
        char* buf=new char[cmd_size];
        cmd.serialize(buf,cmd_size);
        data.write(buf,cmd_size);

        recovery_log.push_back(empty);

        #ifdef DEBUG
        data.seekp(0, std::ios::beg);
        meta.seekp(0, std::ios::beg);
        int t=99,v=99;
        meta.read(reinterpret_cast<char *>(&t), sizeof(t));
        meta.read(reinterpret_cast<char *>(&v), sizeof(v));
        // meta>>t;meta>>v;
        std::cout<<"test after empty start:"<<t<<" "<<v<<std::endl;
        #endif
    }
    else if(!is_empty(data) && !is_empty(meta))
    {
        #ifdef DEBUG
        std::cout<<"not empty start"<<std::endl;
        #endif
        data.seekp(0, std::ios::beg);
        meta.seekp(0, std::ios::beg);

        int log_num=0;

        meta.read(reinterpret_cast<char *>(&current_term), sizeof(current_term));
        meta.read(reinterpret_cast<char *>(&voted_for), sizeof(voted_for));
        meta.read(reinterpret_cast<char *>(&log_num), sizeof(log_num));

        #ifdef DEBUG
        std::cout<<"node:"<<my_id<<" after recovery current term:"<<current_term<<" vote_for:"<<voted_for<<" log_num:"<<log_num<<" ";
        #endif

        int cnt=0;
        while(!data.eof()&&cnt<log_num)
        {
            int cur_term;
            command cur_cmd;
            int cmd_size=cur_cmd.size();
            char* buf=new char[cmd_size];
            data.read(reinterpret_cast<char *>(&cur_term), sizeof(cur_term));
            data.read(buf,cmd_size);
            cur_cmd.deserialize(buf,cmd_size);
            delete buf;

            #ifdef DEBUG
            std::cout<<cur_term<<":"<<cur_cmd.value<<" ";
            #endif

            recovery_log.push_back(log_entry<command>(cur_term,cur_cmd));
            cnt++;
        }

        #ifdef DEBUG
        std::cout<<std::endl;
        #endif
    }
    else
    {
        assert(0);
    }
    meta.flush();
    data.flush();
    #ifdef DEBUG
    std::cout<<"node:"<<my_id<<" after recovery current term:"<<current_term<<" vote_for:"<<voted_for<<" log size:"<<recovery_log.size()<<std::endl;
    #endif
}

template<typename command>
void raft_storage<command>::persist_current_term(int current_term)
{
    std::unique_lock<std::mutex> lock(mtx);
    #ifdef DEBUG
    std::cout<<"node:"<<my_id<<" persist current term:"<<current_term<<std::endl;
    #endif
    meta.seekp(0, std::ios::beg);
    meta.write(reinterpret_cast<const char *>(&current_term), sizeof(current_term));
    meta.flush();
}

template<typename command>
void raft_storage<command>::persist_voted_for(int voted_for)
{
    std::unique_lock<std::mutex> lock(mtx);
    #ifdef DEBUG
    std::cout<<"node:"<<my_id<<" persist vote for:"<<voted_for<<std::endl;
    #endif
    meta.seekp(sizeof(int), std::ios::beg);
    meta.write(reinterpret_cast<const char *>(&voted_for), sizeof(voted_for));
    meta.flush();
}

// template<typename command>
// void raft_storage<command>::persist_entry(log_entry<command> entry)
// {
//     std::unique_lock<std::mutex> lock(mtx);
//     std::ifstream data(filename,std::ios::binary|std::ios::app);
//     data<<entry.term;
//     command cmd=entry.cmd;
//     int cmd_size=cmd.size();
//     char* buf=new char[cmd_size];
//     cmd.serialize(buf,cmd_size);
//     data.write(buf,cmd_size);
//     delete buf;
//     data.close();
// }

template<typename command>
void raft_storage<command>::persist_log(std::vector<log_entry<command>> log)
{
    std::unique_lock<std::mutex> lock(mtx);
    data.seekp(0, std::ios::beg);
    #ifdef DEBUG
    std::cout<<"node:"<<my_id<<" persist log:";
    #endif
    int log_num=(int)log.size();
    meta.seekp(2*sizeof(int),std::ios::beg);
    meta.write(reinterpret_cast<const char *>(&log_num), sizeof(log_num));
    // update log num
    for(auto& entry:log)
    {
        data.write(reinterpret_cast<const char *>(&entry.term), sizeof(entry.term));
        command cmd=entry.cmd;
        int cmd_size=cmd.size();
        char* buf=new char[cmd_size];
        cmd.serialize(buf,cmd_size);
        data.write(buf,cmd_size);
        delete buf;
        
        #ifdef DEBUG
        std::cout<<entry.term<<":"<<entry.cmd.value<<" ";
        #endif
    }
    #ifdef DEBUG
    std::cout<<std::endl;
    #endif
    data.flush();
    meta.flush();
}

template<typename command>
bool raft_storage<command>::is_empty(std::fstream& file)
{
    file.seekp(0,std::ios::end);
    size_t size = file.tellg();
    return size==0;
}

#endif // raft_storage_h