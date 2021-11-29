#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return sizeof(command_type)+sizeof(int)+key.size()+value.size();
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    // key size(int),key,value
    if(size!=this->size()) return;
    memcpy(buf,&cmd_tp,sizeof(command_type));
    int key_size=key.size();
    //key cannot be empty
    assert(key_size>=0);
    memcpy(buf+sizeof(command_type),&key_size,sizeof(key_size));
    if(key.size()!=0)
    {
        memcpy(buf+sizeof(command_type)+sizeof(int),key.c_str(),key.size());
    }
    if(value.size()!=0)
    {
        memcpy(buf+sizeof(command_type)+sizeof(int)+key.size(),value.c_str(),value.size());
    }
    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
    memcpy(&cmd_tp,buf,sizeof(cmd_tp));
    int key_size=0;
    memcpy(&key_size,buf+sizeof(cmd_tp),sizeof(key_size));

    if(key_size==0)
    {
        key="";
    }
    else
    {
        char* key_raw=new char[key_size+1];
        memcpy(key_raw,buf+sizeof(cmd_tp)+sizeof(key_size),key_size);
        key_raw[key_size]='\0';
        key=std::string(key_raw);
        delete [] key_raw;
    }

    int value_size=size-(sizeof(command_type)+sizeof(int)+key_size);
    assert(value_size>=0);
    
    if(value_size==0)
    {
        value="";
    }
    else{
        char* value_raw=new char[value_size+1];
        memcpy(value_raw,buf+sizeof(cmd_tp)+sizeof(key_size)+key_size,value_size);
        value_raw[value_size]='\0';
        value=std::string(value_raw);
        delete [] value_raw;
    }

    return;
}

int kv_command::type_to_int(command_type type)
{
    switch(type)
    {
        case command_type::CMD_NONE:
        return 0;
        case command_type::CMD_GET:
        return 1;
        case command_type::CMD_PUT:
        return 2;
        case command_type::CMD_DEL:
        return 3;
        default:
        //error
        assert(0);
    }
    return -1;
}

kv_command::command_type kv_command::int_to_type(int integer)
{
    switch(integer)
    {
        case 0:
        return command_type::CMD_NONE;
        case 1:
        return command_type::CMD_GET;
        case 2:
        return command_type::CMD_PUT;
        case 3:
        return command_type::CMD_DEL;
        default:
        assert(0);
    }
    // error happen
    return command_type::CMD_NONE;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    m<<kv_command::type_to_int(cmd.cmd_tp);
    m<<cmd.key;
    m<<cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int integer_type;
    u>>integer_type;
    cmd.cmd_tp=kv_command::int_to_type(integer_type);
    u>>cmd.key;
    u>>cmd.value;
    return u;
}

kv_state_machine::kv_state_machine()
{

}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->key=kv_cmd.key;
    if(kv_cmd.cmd_tp==kv_command::CMD_GET)
    {
        auto it=store.find(kv_cmd.key);
        if(it==store.end())
        {
            kv_cmd.res->succ=false;
            kv_cmd.res->value="";
        }
        else
        {
            kv_cmd.res->succ=true;
            kv_cmd.res->value=(*it).second.value;
        }
    }
    else if(kv_cmd.cmd_tp==kv_command::CMD_DEL)
    {
        auto it=store.find(kv_cmd.key);
        if(it==store.end())
        {
            kv_cmd.res->succ=false;
            kv_cmd.res->value="";
        }
        else
        {
            kv_cmd.res->succ=true;
            kv_cmd.res->value=(*it).second.value;
            store.erase(it);
        }
    }
    else if(kv_cmd.cmd_tp==kv_command::CMD_PUT)
    {
        auto it=store.find(kv_cmd.key);
        if(it==store.end())
        {
            kv_cmd.res->succ=true;
            kv_cmd.res->value=kv_cmd.value;
            store.insert(std::pair<std::string,kv_command>(kv_cmd.key,kv_cmd));
        }
        else
        {
            kv_cmd.res->succ=false;
            kv_cmd.res->value=(*it).second.value;
            (*it).second.value=kv_cmd.value;//replace with new value
        }
    }

    kv_cmd.res->done = true;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    return;    
}
