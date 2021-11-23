#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int commit_index;
    int last_applied;
    unsigned long last_receive_rpc_time; // last heartbeat time
    int vote_receive;

    unsigned long election_timeout;
    bool is_voted;  // is voted in this term


private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    unsigned long get_current_time();
    inline void set_role(raft_role role){this->role=role;}
    inline raft_role get_role(){return this->role;}
    inline void reset_election_timeout(){this->election_timeout=300+rand()%200;}
    inline unsigned long get_election_timeout(){return this->election_timeout;}

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    commit_index(0),
    last_applied(0),
    last_receive_rpc_time(0),
    vote_receive(0),
    is_voted(false)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    srand(time(NULL));
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:

    term = current_term;
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // Your code here:
    mtx.lock();

    // reply term
    reply.term=std::max(args.term,this->current_term);
    reply.vote_granted=false;

    // for all servers
    if(args.term>this->current_term)
    {
        this->current_term=args.term;
        set_role(raft_role::follower);
        is_voted=false; //not vote for the incoming term
        // must not return here
        RAFT_LOG("node:%d in request_vote become follower request term:%d",this->my_id,args.term);
    }

    // if follower
    if(this->get_role()==raft_role::follower)
    {
        if(!is_voted)
        {
            is_voted=true;
            reply.vote_granted=true;
            this->last_receive_rpc_time=this->get_current_time();//todo
        }
    }


    // RAFT_LOG("node:%d are required to vote for candidate:%d",this->my_id,args.candidate_id);



    mtx.unlock();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    mtx.lock();

    // for all servers
    if(reply.term>this->current_term)
    {
        this->current_term=reply.term;
        set_role(raft_role::follower);
        RAFT_LOG("node:%d in handle_request_vote_reply become follower",this->my_id);
    }

    if(this->get_role()==raft_role::candidate)
    {
        // agree to vote
        if(reply.vote_granted==true)
        {
            this->vote_receive++;
            RAFT_LOG("node:%d reply_term:%d election support from node:%d",this->my_id,reply.term,target);
            // receive majority votes and become leader
            if(this->vote_receive>this->num_nodes()/2)
            {
                this->set_role(raft_role::leader);
                RAFT_LOG("node:%d become leader role:%d in term:%d",this->my_id,this->get_role(),this->current_term);
                // first ping happen immediately after become a leader
                int node_num=this->num_nodes();
                append_entries_args<command> arg(this->current_term,this->my_id,this->commit_index,this->last_applied,std::vector<log_entry<command>>(),this->commit_index);
                RAFT_LOG("ping");
                for(int i=0;i<node_num;++i)
                {
                    if(i==this->my_id) continue;
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
            }
        }
    }
    
    mtx.unlock();
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    mtx.lock();
    RAFT_LOG("node:%d heartbeat from leader:%d term:%d",this->my_id,arg.leader_id,arg.term);

    // reply term
    reply.term=std::max(arg.term,this->current_term);

    // for all servers
    if(arg.term>this->current_term)
    {
        this->current_term=arg.term;
        set_role(raft_role::follower);
        RAFT_LOG("node:%d in heartbeat become follower",this->my_id);
    }

    //heartbeat from new leader
    if(this->get_role()==raft_role::candidate)
    {
        this->current_term=arg.term;
        set_role(raft_role::follower);
        RAFT_LOG("node:%d in heartbeat become follower",this->my_id);
    }

    this->last_receive_rpc_time=this->get_current_time();

    mtx.unlock();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    mtx.lock();
    // for all server
    if(reply.term>this->current_term)
    {
        RAFT_LOG("node:%d in handle_append_entries_reply become follower",this->my_id);
        this->current_term=reply.term;
        this->set_role(raft_role::follower);
    }
    mtx.unlock();
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    
    const int candidate_election_timeout=1000;
    unsigned long last_election_begin_time=0;

    reset_election_timeout();
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        // check leader liveness
        mtx.lock();

        if(this->get_role()!=raft_role::leader)
        {
            unsigned long now=this->get_current_time();
            if((now-this->last_receive_rpc_time<get_election_timeout())
                ||(this->get_role()==raft_role::candidate && now-last_election_begin_time<candidate_election_timeout))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            else
            {
                // begin new election if no heartbeat or election timeout
                reset_election_timeout();
        
                last_election_begin_time=get_current_time();
                // as a follower start election
                this->current_term++;
                // become candidate
                RAFT_LOG("node:%d become candidate",this->my_id);
                this->set_role(raft_role::candidate);
                //vote for itself
                this->vote_receive=1;
                this->is_voted=true;
                request_vote_args args(this->current_term,this->my_id,this->commit_index,this->last_applied);

                int node_num=this->num_nodes();
                for(int i=0;i<node_num;++i)
                {
                    if(i==this->my_id) continue;
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
                }
            }
        }
        
        mtx.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:

        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    const unsigned long ping_timeout=150;
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        mtx.lock();
        if(this->get_role()==raft_role::leader)
        {
            RAFT_LOG("ping");
            int node_num=this->num_nodes();
            append_entries_args<command> arg(this->current_term,this->my_id,this->commit_index,this->last_applied,std::vector<log_entry<command>>(),this->commit_index);
            for(int i=0;i<node_num;++i)
            {
                if(i==this->my_id) continue;
                thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(ping_timeout)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/
template<typename state_machine, typename command>
unsigned long raft<state_machine, command>::get_current_time()
{
    unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::system_clock::now().time_since_epoch()).count();
    return now;
}




#endif // raft_h