#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    // TODO: Your code here

    // if request not succeed, wait for notification
    if(!db->lm.lock_request(key,tx_id))
    {
        #if DEBUG
        std::cout<<"["<<tx_id<<"]"<<"(wait put) key:"<<key<<std::endl;
        #endif
        std::unique_lock<std::mutex> lck_g(db->lm.cv_m);
        db->lm.condVar.wait(lck_g,[=]{return (db->lm.tx_ntf.tx_id_g==tx_id);});
        db->lm.tx_ntf.mtx.unlock();
    }
    #if DEBUG
    std::cout<<"["<<tx_id<<"]"<<"(continue put) key:"<<key<<" tx_id_g:"<<db->lm.tx_ntf.tx_id_g<<std::endl;
    #endif
    // send to viewserver
    chdb_protocol::operation_var oper(this->tx_id,key,val);
    int response=-1;
    db->vserver->execute(key,chdb_protocol::Put,oper,response);
    //avoid duplicate lock
    if(!keys_set.count(key))
    {
        access_order.push_back(key);
        keys_set.insert(key);
    }
    return 0;
}

int tx_region::get(const int key) {
    // TODO: Your code here

     // if request not succeed, wait for notification
    if(!db->lm.lock_request(key,tx_id))
    {
        #if DEBUG
        std::cout<<"["<<tx_id<<"]"<<"(wait get) key:"<<key<<std::endl;
        #endif
        std::unique_lock<std::mutex> lck_g(db->lm.cv_m);
        db->lm.condVar.wait(lck_g,[=]{return (db->lm.tx_ntf.tx_id_g==tx_id);});
        db->lm.tx_ntf.mtx.unlock();
    }
    #if DEBUG
    std::cout<<"["<<tx_id<<"]"<<"(continue get) key:"<<key<<" tx_id_g:"<<db->lm.tx_ntf.tx_id_g<<std::endl;
    #endif
    chdb_protocol::operation_var oper(this->tx_id,key,-1);
    int get_value=-1;
    db->vserver->execute(key,chdb_protocol::Get,oper,get_value);
    if(!keys_set.count(key))
    {
        access_order.push_back(key);
        keys_set.insert(key);
    }
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
    // db->mtx.lock();
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

    // release lock in reverse order
    while(!access_order.empty())
    {
        int key = access_order.back();
        access_order.pop_back();
        db->lm.unlock_request(key,tx_id);
    }

    printf("tx[%d] commit\n", tx_id);
    // db->mtx.unlock();
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
    // release lock in reverse order
    db->lm.abort_all(tx_id);

    printf("tx[%d] abort\n", tx_id);
    // db->mtx.unlock();
    return 0;
}
