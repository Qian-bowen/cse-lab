#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here
    // send to viewserver
    chdb_protocol::operation_var oper(this->tx_id,key,val);
    int response=-1;
    db->vserver->execute(key,chdb_protocol::Put,oper,response);
    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here
    chdb_protocol::operation_var oper(this->tx_id,key,-1);
    int get_value=-1;
    db->vserver->execute(key,chdb_protocol::Get,oper,get_value);
    return get_value;
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    std::vector<shard_client *> shards_vec = db->shards;
    chdb_protocol::check_prepare_state_var var(this->tx_id);
    int prepare_num=0;
    for(const auto shard:shards_vec)
    {
        int response=-1;
        int port=shard->node->port();
        // std::cout<<"shard info port:"<<port<<" addr:"<<shard->node<<std::endl;
        // std::cout<<"check prepare state"<<std::endl;
        if(db->vserver->node->call(port,chdb_protocol::CheckPrepareState,var,response)==0)
        {
            if(response>0) prepare_num++;
        }
    }
    if(prepare_num==shards_vec.size()) return chdb_protocol::prepare_ok;
    return chdb_protocol::prepare_not_ok;
}

int tx_region::tx_begin() {
    // TODO: Your code here
    db->mtx.lock();
    std::vector<shard_client *> shards_vec = db->shards;
    chdb_protocol::prepare_var var(this->tx_id);
    for(const auto shard:shards_vec)
    {
        int response=-1;
        int port=shard->node->port();
        db->vserver->node->call(port,chdb_protocol::Prepare,var,response);
    }
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    std::vector<shard_client *> shards_vec = db->shards;
    chdb_protocol::commit_var var(this->tx_id);
    for(const auto shard:shards_vec)
    {
        int response=-1;
        int port=shard->node->port();
        db->vserver->node->call(port,chdb_protocol::Commit,var,response);
    }
    printf("tx[%d] commit\n", tx_id);
    db->mtx.unlock();
    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    std::vector<shard_client *> shards_vec = db->shards;
    chdb_protocol::rollback_var var(this->tx_id);
    for(const auto shard:shards_vec)
    {
        int response=-1;
        int port=shard->node->port();
        db->vserver->node->call(port,chdb_protocol::Rollback,var,response);
    }
    printf("tx[%d] abort\n", tx_id);
    db->mtx.unlock();
    return 0;
}
