#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here

    int term,index;
    int leader = raft_group->check_exact_one_leader();
    if(proc==chdb_protocol::Put)
    {
        chdb_command cmd(chdb_command::CMD_PUT,query_key,var.value,var.tx_id);
        raft_group->nodes[leader]->new_command(cmd,term,index);
    }
    else if(proc==chdb_protocol::Get)
    {
        chdb_command cmd(chdb_command::CMD_GET,query_key,var.value,var.tx_id);
        raft_group->nodes[leader]->new_command(cmd,term,index);
    }

    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}

void show_list(std::list<int>& lt)
{
    for(const auto i:lt)
    {
        std::cout<<i<<" ";
    }
    std::cout<<std::endl;
}

bool lock_manager::compatiable(int key,int tx_id)
{
    if((!tx_set[tx_id].empty())
        && (key==tx_set[tx_id].front()))
    {
        // tx_set[tx_id].pop_front();
        return true;
    }
    return false;
}

bool lock_manager::lock_request(int key,int tx_id)
{
    latch.lock();

    #if DEBUG
    std::cout<<"["<<tx_id<<"]"<<"(try lock) key:"<<key<<std::endl;
    #endif

    #if DEBUG
    std::cout<<"before req:";
    for(const auto i:lock_table[key])
    {
        std::cout<<i<<" ";
    }
    std::cout<<std::endl;
    #endif
    
    bool success=false,add_table=true;
    // add tx to end of list, if empty just grant
    // if already granted, just success
    if(lock_table[key].empty()
        ||lock_table[key].front()==tx_id) 
    {
        success=true;
    }

    // check whether the key is already lock by the same tx
    if(!tx_set[tx_id].empty())
    {
        std::list<int> keys=tx_set[tx_id];
        for(const auto k:keys)
        {
            if(k==key)
            {
                // success=true;
                add_table=false;
                break;
            }
        }
    }

    if(add_table)
    {
        lock_table[key].push_back(tx_id);
        tx_set[tx_id].push_back(key);
    }

    #if DEBUG
    std::cout<<"after req:";
    for(const auto i:lock_table[key])
    {
        std::cout<<i<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"success:"<<success<<std::endl;
    #endif

    latch.unlock();
    return success;
}

void lock_manager::unlock_request(int key,int tx_id)
{
    // delete record
    latch.lock();
    #if DEBUG
    std::cout<<"["<<tx_id<<"]"<<"(try unlock) key:"<<key<<std::endl;
    #endif
    
    #if DEBUG
    std::cout<<"before unlock req:";
    for(const auto i:lock_table[key])
    {
        std::cout<<i<<" ";
    }
    std::cout<<std::endl;
    #endif

    assert(!lock_table[key].empty());
    assert(lock_table[key].front()==tx_id);
    if((!lock_table[key].empty())
        && (lock_table[key].front()==tx_id))
    {
        #if DEBUG
        std::cout<<"["<<tx_id<<"]"<<"(unlock) key:"<<key<<std::endl;
        #endif
        lock_table[key].pop_front();
        // test following record and notify
        // if someone waiting, notify it
        if(!lock_table[key].empty())
        {
            int tmp_tx = lock_table[key].front();
            if(compatiable(key,tmp_tx))
            {
                tx_ntf.mtx.lock();
                tx_ntf.tx_id_g=tmp_tx;
                #if DEBUG
                std::cout<<"["<<tmp_tx<<"]"<<"(notifiy) key:"<<key<<std::endl;
                #endif
                condVar.notify_all();
            }
        }
    }
    latch.unlock();
}

void lock_manager::abort_all(int tx_id)
{
    latch.lock();
    #if DEBUG
    std::cout<<"["<<tx_id<<"]"<<"(abort)"<<std::endl;
    #endif
    std::list<int> locks = tx_set[tx_id];
    for(const auto key:locks)
    {
        // delete all key in waiting set
        // if it is locked, unlock(delete) and notify next
        if(!lock_table[key].empty())
        {
            #if DEBUG
            std::cout<<"before key:"<<key<<" | ";
            for(const auto i:lock_table[key])
            {
                std::cout<<i<<" ";
            }
            std::cout<<std::endl;
            #endif

            int first_tmp_tx = lock_table[key].front();
            if(first_tmp_tx==tx_id)
            {
                lock_table[key].pop_front();
                int tmp_tx = lock_table[key].front();
                if(compatiable(key,tmp_tx))
                {
                    tx_ntf.mtx.lock();
                    tx_ntf.tx_id_g=tmp_tx;
                    #if DEBUG
                    std::cout<<"["<<tmp_tx<<"]"<<"(notifiy) key:"<<key<<std::endl;
                    #endif
                    condVar.notify_all();
                }
            }
            std::list<int> waiting_list=lock_table[key];
            auto iter=waiting_list.begin();
            iter++;
            for(;iter!=waiting_list.end();++iter)
            {
                if(*iter==tx_id)
                {
                    waiting_list.erase(iter);
                }
            }

            #if DEBUG
            std::cout<<"after key:"<<key<<" | ";
            for(const auto i:lock_table[key])
            {
                std::cout<<i<<" ";
            }
            std::cout<<std::endl;
            #endif
        }
    }
    tx_set.erase(tx_id);
    latch.unlock();
}
