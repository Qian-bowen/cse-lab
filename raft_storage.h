#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void set_filename(int myid);
    void persist_to_file();
private:
    std::mutex mtx;
    std::string dir_name;
    std::string filename;
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

template<typename command>
void raft_storage<command>::set_filename(int myid)
{
    filename=dir_name + "/raft_storage_"  + std::to_string(myid);
}

template<typename command>
void raft_storage<command>::persist_to_file()
{
    
}

#endif // raft_storage_h