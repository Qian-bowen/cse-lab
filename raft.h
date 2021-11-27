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

#define DEBUG
#define JUDGE

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
    int voted_for;  // voted for which candidate in this term, -1 means none
    unsigned long candidate_election_timeout;
    unsigned long last_election_begin_time;

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
    // do not reset in loop! rand() is time consuming 
    // avoid rand() possible
    // Attention! too much node may have conflict
    inline void reset_election_timeout(){this->election_timeout=300+rand()%200;}
    inline unsigned long get_election_timeout(){return this->election_timeout;}
    inline void reset_candidate_election_timeout(){this->candidate_election_timeout=900+(10*my_id)%200;}
    inline unsigned long get_candidate_election_timeout(){return this->candidate_election_timeout;}
    int majority_element(std::vector<int>& nums,bool& have_result);
    bool is_majority_reachable(std::vector<rpcc*>& clients);
    // int num_reachable_nodes(std::vector<rpcc*>& clients);

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
    voted_for(-1),
    last_election_begin_time(0)
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
    #ifdef DEBUG
    RAFT_LOG("start");
    #endif
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    std::unique_lock<std::mutex> lock(mtx);

    bool is_leader_reachable=false;
    {
        // leader and half reachable
        is_leader_reachable=(this->get_role()==raft_role::leader);
        // only leader can append cmd from client
        if(is_leader_reachable)
        {
            log.push_back(log_entry<command>(this->current_term,cmd));
            #ifdef DEBUG
            RAFT_LOG("leader receive cmd max index:%d value:%d",(int)log.size()-1,cmd.value);
            #endif
            // leader commit is valid when the majority commit
            // update next_index match_index for itself
            match_index[my_id]=log.size()-1;

            term = current_term;
            index = (int)log.size()-1; // index is log index
        }
    }
    // mtx.unlock();
    return is_leader_reachable;
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
    std::unique_lock<std::mutex> lock(mtx);
    {

        reply.vote_granted=false;
        reply.term=this->current_term;

        // for all servers
        if(args.term<current_term)
        {
            reply.vote_granted=false;
        }
        else if(args.term>this->current_term)
        {
            this->current_term=args.term;
            set_role(raft_role::follower);
            // must not return here
            #ifdef DEBUG
            RAFT_LOG("node:%d in request_vote become follower request term:%d",this->my_id,args.term);
            #endif
            voted_for=-1;
            // reply.vote_granted=true;
            // voted_for=args.candidate_id;
            // reply.vote_granted=true;
        }
        //  If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
        // decide to vote
        // if my log is newer, refuse to vote
        if((log.back().term<args.last_log_term)
            ||((log.back().term==args.last_log_term)&&((int)log.size()-1<=args.last_log_index)))
        {
            if(voted_for==-1)
            {
                #ifdef DEBUG
                RAFT_LOG("node:%d vote for node:%d in term:%d",this->my_id,args.candidate_id,args.term);
                #endif
            
                voted_for=args.candidate_id;
                reply.vote_granted=true;
            }
        }
        

        // important! update receive rpc time!
        this->last_receive_rpc_time=this->get_current_time();
    }
    // mtx.unlock();
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    {
        if(reply.term>this->current_term)
        {
            this->current_term=reply.term;
            set_role(raft_role::follower);
            #ifdef DEBUG
            RAFT_LOG("node:%d in handle_request_vote_reply become follower",this->my_id);
            #endif
            voted_for=-1;
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
                // can become a leader of a partition network if receive majority votes in the partition
                if(this->vote_receive>this->num_nodes()/2)
                {
                    this->set_role(raft_role::leader);
                    #ifdef DEBUG
                    RAFT_LOG("node:%d become leader role:%d in term:%d",this->my_id,this->get_role(),this->current_term);
                    #endif
                    // RAFT_LOG("node:%d become leader role:%d in term:%d",this->my_id,this->get_role(),this->current_term);
                    // Reinitialized after election
                    // next index initialize to leader last log index + 1, namely log.size()-1+1
                    next_index.resize(next_index.size(),log.size());
                    match_index.resize(match_index.size(),0);
                }
            }
        }
    }
    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    {
        // RAFT_LOG("node:%d heartbeat from leader:%d term:%d",this->my_id,arg.leader_id,arg.term);
        reply.success=true;

        // reply term, for leader to update itself
        reply.term=this->current_term;

        // for all servers
        if(arg.term>this->current_term)
        {
            this->current_term=arg.term;
            set_role(raft_role::follower);
            #ifdef DEBUG
            RAFT_LOG("node:%d become follower",this->my_id);
            #endif
            voted_for=-1;//new 
        }

        #ifdef JUDGE
        assert(arg.prev_log_index>=0);
        #endif
      
        if(arg.term<this->current_term)
        {
            reply.success=false;
        }

        // if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        // log consistence checking for all servers
        else if(arg.prev_log_index>=(int)this->log.size()
            || (this->log[arg.prev_log_index].term!=arg.prev_log_term))
        {
            #ifdef JUDGE
            assert(arg.prev_log_index!=0);
            #endif
            reply.success=false;
        }

  
        /**
         * @brief If an existing entry conflicts with a new one (same index but different terms), 
         * delete the existing entry and all that follow it
         * Append any new entries not already in the log
         */



        // append any new
        
        // can only modify log when success
        if(reply.success&&(int)arg.entries.size()>0)
        {
            log.erase(log.begin()+1+arg.prev_log_index,log.end());
            log.insert(log.end(),arg.entries.begin(),arg.entries.end());
        }

        #ifdef DEBUG
        RAFT_LOG("append term:%d leader_id:%d prev_log_index:%d prev_log_term:%d leader_commit:%d entry size:%d",arg.term,arg.leader_id,arg.prev_log_index,arg.prev_log_term,arg.leader_commit,(int)arg.entries.size());
        RAFT_LOG("log max index:%d",(int)log.size()-1);
        #endif

        /**
         * @brief in normal situation, leader commit is always le than follower commit index, but not hold for partition
         * A,B,C,D,E for origin, A is leader, all commit index is 1
         * now partition to group A,B and group C,D,E, C become leader and C,D,E commit to 6
         * when rejoin, before  A konws it's not leader, it send commit index 1 to C
         * not success and commit index of C should still be 6
         */
        #ifdef JUDGE
        // assert( (!reply.success) || (arg.leader_commit>=commit_index));
        #endif
        
        if(reply.success && (arg.leader_commit>commit_index))
        {
            // get min
            commit_index=std::min(arg.leader_commit,(int)log.size()-1);
            #ifdef DEBUG
            RAFT_LOG("update commit index to:%d",commit_index);
            #endif
        }

        #ifdef DEBUG
        if(commit_index>=(int)log.size())
        {
            RAFT_LOG("commit index:%d log max index:%d",commit_index,(int)log.size()-1);
        }
        #endif

        // assert(commit_index<(int)log.size());
        #ifdef JUDGE
        assert(commit_index<(int)log.size());
        #endif
        

        this->last_receive_rpc_time=this->get_current_time();
    }
  
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    {
        // for all server
        if(reply.term>this->current_term)
        {
            #ifdef DEBUG
            RAFT_LOG("node:%d in handle_append_entries_reply become follower",this->my_id);
            #endif
            this->current_term=reply.term;
            this->set_role(raft_role::follower);
            voted_for=-1;//new 
        }

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

                // speed up
                int reply_term=reply.term;
                if(reply_term<log[next_index[target]].term)
                {
                    while(reply_term<log[next_index[target]].term)
                    {
                        next_index[target]--;
                    }
                    next_index[target]++;
                }
            }
            // If successful: update nextIndex and matchIndex for follower
            else
            {
                #ifdef DEBUG
                RAFT_LOG("handle entry reply target:%d term:%d success:%d entry size:%d",target,reply.term,reply.success,(int)arg.entries.size());
                #endif
                next_index[target]=arg.prev_log_index+arg.entries.size()+1;
                match_index[target]=arg.prev_log_index+arg.entries.size();
            }
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

    reset_election_timeout();
    
    while (true) {
        if (is_stopped()) return;
        // Your code here
        {
            std::unique_lock<std::mutex> lock(mtx);
            unsigned long now=this->get_current_time();

            // cannot start new election until timeout
            if(((this->get_role()==raft_role::candidate)
                && (now-last_election_begin_time>get_candidate_election_timeout()))
                ||
                ((this->get_role()==raft_role::follower)
                && (now-this->last_receive_rpc_time>get_election_timeout())))
            {
                /**
                 * @brief start an election if only the majority is reachable
                 * imagine
                 */
                if(is_majority_reachable(this->rpc_clients))
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
                    this->voted_for=my_id;
                    request_vote_args args(this->current_term,this->my_id,this->log.size()-1,this->log.back().term);

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
                std::vector<int> maj(match_index);
                sort(maj.begin(), maj.end());
                int N=maj[num_nodes()/2];
                if(N>commit_index&&log[N].term==current_term){
                    commit_index=N;
                }

                // send log
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
                        #ifdef JUDGE
                        assert((next_index[i]-1<(int)log.size())&&(next_index[i]-1>=0));
                        #endif
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
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(commit_index>last_applied)
            {
                last_applied++;
                #ifdef JUDGE
                assert(last_applied<(int)log.size());
                #endif
                state->apply_log(log[last_applied].cmd);
                #ifdef DEBUG
                RAFT_LOG("apply index:%d",last_applied);
                #endif
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    const unsigned long ping_timeout=100;
    while (true) {
        if (is_stopped()) return;
        {
            std::unique_lock<std::mutex> lock(mtx);
            if(this->get_role()==raft_role::leader)
            {
                // RAFT_LOG("ping");
                int node_num=this->num_nodes();
                for(int i=0;i<node_num;++i)
                {
                    if(i==this->my_id) continue;
                    #ifdef JUDGE
                    assert(next_index[i]-1<(int)log.size());
                    assert((next_index[i]-1>=0));
                    #endif
                    std::vector<log_entry<command>> empty_ping;
                    append_entries_args<command> arg(this->current_term,this->my_id,next_index[i]-1,this->log[next_index[i]-1].term,empty_ping,this->commit_index);
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, arg);
                }
            }
        }
        
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


/**
 * @brief whether the node can reach the majority
 * @tparam state_machine 
 * @tparam command 
 * @param clients 
 * @return true 
 * @return false 
 */
template<typename state_machine, typename command>
bool raft<state_machine, command>::is_majority_reachable(std::vector<rpcc*>& clients)
{
    int cnt=0;
    for(const auto& client:clients)
    {
        if(client->reachable()) cnt++;
    }
    if(cnt>num_nodes()/2)
    {
        return true;
    }
    return false;
}




#endif // raft_h