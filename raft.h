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

// #define DEBUG 0

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
    unsigned long candidate_election_timeout;

    std::vector<log_entry<command>> log; // pair of term and command
    std::vector<int> next_index;
    std::vector<int> match_index;


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
    inline void reset_candidate_election_timeout(){this->candidate_election_timeout=900+rand()%200;}
    inline unsigned long get_candidate_election_timeout(){return this->candidate_election_timeout;}
    int majority_element(std::vector<int>& nums,bool& have_result);

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
    // next index initialize to last log index +1
    // and the first log index 1
    next_index.resize(rpc_clients.size(),1);
    match_index.resize(rpc_clients.size(),0);
    log.push_back(log_entry<command>(0)); // log index begin from zero ,so add empty entry
    // wrong code to be deleted
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
    // mtx.lock();
    bool is_leader=false;
    {
        std::unique_lock<std::mutex> lock(mtx);
        is_leader=(this->get_role()==raft_role::leader);
        // only leader can append cmd from client
        if(is_leader)
        {
            log.push_back(log_entry<command>(this->current_term,cmd));
            #ifdef DEBUG
            RAFT_LOG("leader receive cmd max index:%d value:%d",(int)log.size()-1,cmd.value);
            #endif
            // leader commit is valid when the majority commit
            // update next_index match_index for itself
            next_index[my_id]=commit_index+1;
            match_index[my_id]=log.size()-1;
        }
        term = current_term;
        index = (int)log.size()-1; // index is log index
    }
    // mtx.unlock();
    return is_leader;
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
    // mtx.lock();
    {
        std::unique_lock<std::mutex> lock(mtx);
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
            #ifdef DEBUG
            RAFT_LOG("node:%d in request_vote become follower request term:%d",this->my_id,args.term);
            #endif
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
    }
    // mtx.unlock();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    // mtx.lock();
    {
        std::unique_lock<std::mutex> lock(mtx);
        // for all servers
        if(reply.term>this->current_term)
        {
            this->current_term=reply.term;
            set_role(raft_role::follower);
            #ifdef DEBUG
            RAFT_LOG("node:%d in handle_request_vote_reply become follower",this->my_id);
            #endif
        }

        if(this->get_role()==raft_role::candidate)
        {
            // agree to vote
            if(reply.vote_granted==true)
            {
                this->vote_receive++;
                #ifdef DEBUG
                RAFT_LOG("node:%d reply_term:%d election support from node:%d",this->my_id,reply.term,target);
                #endif
                // receive majority votes and become leader
                if(this->vote_receive>this->num_nodes()/2)
                {
                    this->set_role(raft_role::leader);
                    #ifdef DEBUG
                    RAFT_LOG("node:%d become leader role:%d in term:%d",this->my_id,this->get_role(),this->current_term);
                    #endif
                    // Reinitialized after election
                    // next index initialize to leader last log index + 1, namely log.size()-1+1
                    next_index.resize(next_index.size(),log.size());
                    match_index.resize(match_index.size(),0);

                    // first ping happen immediately after become a leader
                    int node_num=this->num_nodes();
                    // RAFT_LOG("ping");
                    for(int i=0;i<node_num;++i)
                    {
                        if(i==this->my_id) continue;
                        // assert(next_index[i]-1<(int)log.size());
                        append_entries_args<command> arg(this->current_term,this->my_id,next_index[i]-1,this->log[next_index[i]-1].term,std::vector<log_entry<command>>(),this->commit_index);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    }
                }
            }
        }
    }
    // mtx.unlock();
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    // mtx.lock();
    {
        std::unique_lock<std::mutex> lock(mtx);
        // RAFT_LOG("node:%d heartbeat from leader:%d term:%d",this->my_id,arg.leader_id,arg.term);
        reply.success=true;
        // not a heartbeat
        if(arg.entries.size()!=0)
        {
            #ifdef DEBUG
            RAFT_LOG("append term:%d leader_id:%d prev_log_index:%d prev_log_term:%d leader_commit:%d entry size:%d",arg.term,arg.leader_id,arg.prev_log_index,arg.prev_log_term,arg.leader_commit,(int)arg.entries.size());
            RAFT_LOG("log max index:%d",(int)log.size()-1);
            #endif
            if(arg.term<this->current_term)
            {
                reply.success=false;
            }
            // if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
            else if(arg.prev_log_index!=0 && arg.prev_log_index<(int)this->log.size() && this->log[arg.prev_log_index].term!=arg.prev_log_term)
            {
                reply.success=false;
            }
            else
            {
                std::vector<log_entry<command>> log_vec = arg.entries;
                if(!log_vec.empty())
                {
                    /**
                     * If an existing entry conflicts with a new one (same index
                     * but different terms), delete the existing entry and all that
                     * follow it
                     */
                    // delete conflict and add new, for simplicity, delete all here
                    // do not delete 0 because its an empty entry
                    log.erase(log.begin()+1+arg.prev_log_index,log.end());
                    log.insert(log.end(),arg.entries.begin(),arg.entries.end());
                }
            }
        }

        // can update in heartbeat, so that server can apply immediately
        if(arg.leader_commit>commit_index)
        {
            // get min
            commit_index=std::min(arg.leader_commit,(int)log.size()-1);
            #ifdef DEBUG
            RAFT_LOG("update commit index to:%d",commit_index);
            #endif
        }
        // reply term
        reply.term=std::max(arg.term,this->current_term);

        // for all servers
        if(arg.term>this->current_term)
        {
            this->current_term=arg.term;
            set_role(raft_role::follower);
            #ifdef DEBUG
            RAFT_LOG("node:%d in heartbeat become follower",this->my_id);
            #endif
        }

        //heartbeat from new leader
        if(this->get_role()==raft_role::candidate)
        {
            this->current_term=arg.term;
            set_role(raft_role::follower);
            #ifdef DEBUG
            RAFT_LOG("node:%d in heartbeat become follower",this->my_id);
            #endif
        }

        // update high_log_entry_index in reply
        reply.high_log_entry_index=this->log.size()-1;

        this->last_receive_rpc_time=this->get_current_time();
    }
    // mtx.unlock();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    // mtx.lock();
    {
        std::unique_lock<std::mutex> lock(mtx);
        // RAFT_LOG("handle entry reply target:%d term:%d success:%d",target,reply.term,reply.success);
        if(this->get_role()==raft_role::leader)
        {
            // If AppendEntries fails because of log inconsistency 
            // decrement nextIndex and retry
            if(reply.success==false)
            {
                #ifdef DEBUG
                RAFT_LOG("retry");
                #endif
                next_index[target]--;
                // assert(next_index[target]>=0);
                std::vector<log_entry<command>> send_log;
                if((int)log.size()-1>=next_index[target])
                {
                    send_log.insert(send_log.begin(),log.begin()+next_index[target],log.end());
                }
                // assert(next_index[target]-1<(int)log.size());
                append_entries_args<command> arg_retry(this->current_term,this->my_id,next_index[target]-1,this->log[next_index[target]-1].term,send_log,this->commit_index);
                thread_pool->addObjJob(this, &raft::send_append_entries, target, arg_retry);
            }
            // If successful: update nextIndex and matchIndex for follower
            // not for ping
            else if(arg.entries.size()!=0)
            {
                #ifdef DEBUG
                RAFT_LOG("handle entry reply target:%d term:%d success:%d entry size:%d",target,reply.term,reply.success,(int)arg.entries.size());
                #endif
                // RAFT_LOG("target:%d prev next index:%d",target,next_index[target]);
                next_index[target]=reply.high_log_entry_index+1;
                // RAFT_LOG("target:%d after next index:%d",target,next_index[target]);
                match_index[target]=reply.high_log_entry_index;
            }
        }
        // for all server
        if(reply.term>this->current_term)
        {
            #ifdef DEBUG
            RAFT_LOG("node:%d in handle_append_entries_reply become follower",this->my_id);
            #endif
            this->current_term=reply.term;
            this->set_role(raft_role::follower);
        }
    }
    // mtx.unlock();
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
    
    unsigned long last_election_begin_time=0;

    reset_election_timeout();
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        // attention! lock race problem
        // mtx.lock();
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(this->get_role()!=raft_role::leader)
            {
                unsigned long now=this->get_current_time();
                if((now-this->last_receive_rpc_time<get_election_timeout())
                    ||(this->get_role()==raft_role::candidate && now-last_election_begin_time<get_candidate_election_timeout()))
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
                else
                {
                    // begin new election if no heartbeat or election timeout
                    reset_election_timeout();
                    reset_candidate_election_timeout();
            
                    last_election_begin_time=get_current_time();
                    // as a follower start election
                    this->current_term++;
                    // become candidate
                    #ifdef DEBUG
                    RAFT_LOG("node:%d become candidate",this->my_id);
                    #endif
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
        }
        
        // mtx.unlock();
        
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
        // mtx.lock();
        {
            std::unique_lock<std::mutex> lock(mtx);

            if(this->get_role()==raft_role::leader)
            {
                // commit when majority agree
                // leader commit first
                bool result=true;
                int major_index=majority_element(match_index,result);
                if(result&& major_index>commit_index)
                {
                    #ifdef DEBUG
                    RAFT_LOG("majority index:%d max_index:%d commit index:%d",major_index,(int)log.size()-1,commit_index);
                    #endif
                    for(int i=commit_index+1;i<=major_index;++i)
                    {
                        // assert(i<(int)log.size());
                        if(log[i].term==current_term)
                        {
                            commit_index=i;
                            #ifdef DEBUG
                            RAFT_LOG("final leader commit:%d",commit_index);
                            #endif
                        }
                    }
                }

                // // send log
                int node_num=this->num_nodes();
                
                for(int i=0;i<node_num;++i)
                {
                    if(i==this->my_id) continue;
                    std::vector<log_entry<command>> send_log;
                    // If last log index ≥ nextIndex for a follower
                    // send AppendEntries RPC with log entries starting at nextIndex
                    if((int)log.size()-1>=next_index[i])
                    {
                        // RAFT_LOG("leader max index:%d target:%d target next index:%d",(int)log.size()-1,i,next_index[i]);
                        send_log.insert(send_log.begin(),log.begin()+next_index[i],log.end());
                        #ifdef DEBUG
                        RAFT_LOG("send log entry num:%d to %d",(int)send_log.size(),i);
                        #endif
                        // assert(next_index[i]-1<(int)log.size());
                        append_entries_args<command> arg(this->current_term,this->my_id,next_index[i]-1,this->log[next_index[i]-1].term,send_log,this->commit_index);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                    }
                }
            }
        }
        
        // mtx.unlock();
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
        // mtx.lock();
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(commit_index>last_applied)
            {
                last_applied++;
                // assert(last_applied<(int)log.size());
                state->apply_log(log[last_applied].cmd);
                #ifdef DEBUG
                RAFT_LOG("apply index:%d",last_applied);
                #endif
            }
        }
        // mtx.unlock();
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
        // mtx.lock();
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(this->get_role()==raft_role::leader)
            {
                // RAFT_LOG("ping");
                int node_num=this->num_nodes();
                for(int i=0;i<node_num;++i)
                {
                    if(i==this->my_id) continue;
                    // assert(next_index[i]-1<(int)log.size());
                    append_entries_args<command> arg(this->current_term,this->my_id,next_index[i]-1,this->log[next_index[i]-1].term,std::vector<log_entry<command>>(),this->commit_index);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
            }
        }
        
        // mtx.unlock();
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

/**
 * @brief get the majority element of array
 * Boyer-Moore majority vote algorithm
 * @tparam state_machine 
 * @tparam command 
 * @param nums 
 * @param result 
 * @return true if choose a majority element
 * @return false if no majority element
 */
template<typename state_machine, typename command>
int raft<state_machine, command>::majority_element(std::vector<int>& nums,bool& have_result)
{
    have_result=true;
    int k=0,cand=0;
    for(const auto& num:nums)
    {
        if(k==0){
            cand=num;
            k++;
        }
        else{
            if(num==cand) ++k;
            else --k;
        }
    }

    k=0;
    for(const auto& num:nums)
    {
        if(num==cand) ++k;
    }
    if(k<=(int)nums.size()/2)
    {
        cand=-1;
        have_result=false;
    }
    return cand;
}




#endif // raft_h