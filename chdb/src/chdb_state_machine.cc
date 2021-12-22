#include "chdb_state_machine.h"

chdb_command::chdb_command() {
    // TODO: Your code here
    res=std::make_shared<result>();
    res->start=std::chrono::system_clock::now();
    res->key=key;
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id) {
    // TODO: Your code here
    res=std::make_shared<result>();
    res->start=std::chrono::system_clock::now();
    res->key=key;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here
    res=cmd.res;
    res->start=std::chrono::system_clock::now();
    res->key=key;
}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    memcpy(buf,&key,sizeof(key));
    memcpy(buf+sizeof(key),&value,sizeof(value));
    memcpy(buf+sizeof(key)+sizeof(value),&tx_id,sizeof(tx_id));
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    memcpy(&key,buf,sizeof(key));
    memcpy(&value,buf+sizeof(key),sizeof(value));
    memcpy(&tx_id,buf+sizeof(key)+sizeof(value),sizeof(tx_id));
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    m<<static_cast<int>(cmd.cmd_tp);
    m<<cmd.key;
    m<<cmd.value;
    m<<cmd.tx_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int tp;
    u>>tp;
    cmd.cmd_tp=chdb_command::command_type(tp);;
    u>>cmd.key;
    u>>cmd.value;
    u>>cmd.tx_id;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    chdb_command& ch_cmd=dynamic_cast<chdb_command&>(cmd);
    std::unique_lock<std::mutex> lock(ch_cmd.res->mtx);

    ch_cmd.res->key=ch_cmd.key;

    if(ch_cmd.cmd_tp==chdb_command::CMD_PUT)
    {
        store[ch_cmd.key]=ch_cmd;
    }
    else if(ch_cmd.cmd_tp==chdb_command::CMD_GET)
    {
        if(store.count(ch_cmd.key))
        {
            ch_cmd.res->value=store[ch_cmd.key].value;
        }
    }

    ch_cmd.res->done=true;
    ch_cmd.res->cv.notify_all();
}