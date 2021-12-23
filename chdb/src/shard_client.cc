#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    // get_store()[var.key]=value_entry(var.value);
    //copy
    for(auto& mp:store)
    {
        mp[var.key]=value_entry(var.value);
    }
    // std::cout<<"put:"<<var.key<<" value:"<<get_store()[var.key].value<<std::endl;
    put_log.push_back(var.key);
    read_only=false;
    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    if(get_store().find(var.key)!=get_store().end())
    {
        r=(*get_store().find(var.key)).second.value;
        // std::cout<<"get:"<<var.key<<" value:"<<r<<std::endl;
    }
    else
    {
        std::cout<<"get fail"<<std::endl;
    }
    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    for(auto& mp:store)
    {
        for(const auto& p:put_log)
        {
            mp.erase(p);
        }
    }
    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    if(active||read_only) r=chdb_protocol::prepare_ok;
    else r=chdb_protocol::prepare_not_ok;
    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    put_log.clear();
    read_only=true;
    return 0;
}

shard_client::~shard_client() {
    delete node;
}