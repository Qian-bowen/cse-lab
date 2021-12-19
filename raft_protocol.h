#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc/rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
   OK,
   RETRY,
   RPCERR,
   NOENT,
   IOERR
};

class request_vote_args {
public:
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
public:
    // Your code here
    request_vote_args(int term_,int candidate_id_,int last_log_index_,int last_log_term_)
        :term(term_),candidate_id(candidate_id_),last_log_index(last_log_index_),last_log_term(last_log_term_){}
    request_vote_args()=default;
};

marshall& operator<<(marshall &m, const request_vote_args& args);
unmarshall& operator>>(unmarshall &u, request_vote_args& args);


class request_vote_reply {
public:
    int term;
    bool vote_granted;
public:
    // Your code here
    request_vote_reply(int term,bool vote_granted)
        :term(term),vote_granted(vote_granted){}
    request_vote_reply()=default;
};

marshall& operator<<(marshall &m, const request_vote_reply& reply);
unmarshall& operator>>(unmarshall &u, request_vote_reply& reply);

template<typename command>
class log_entry {
public:
    // Your code here
    int term;
    command cmd;
    log_entry()=default;
    log_entry(int term_):term(term_){cmd=command();} // template variable should also be correctly initialize
    log_entry(int term_, command cmd_):term(term_),cmd(cmd_){}
};

template<typename command>
marshall& operator<<(marshall &m, const log_entry<command>& entry) {
    // Your code here
    m<<entry.term;
    m<<entry.cmd;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, log_entry<command>& entry) {
    // Your code here
    u>>entry.term;
    u>>entry.cmd;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<log_entry<command>> entries;
    int leader_commit;
    append_entries_args(int term_,int leader_id_,int prev_log_index_,int prev_log_term_,std::vector<log_entry<command>> entries_,int leader_commit_)
        :term(term_),leader_id(leader_id_),prev_log_index(prev_log_index_),prev_log_term(prev_log_term_),entries(entries_),leader_commit(leader_commit_){}
    append_entries_args()=default;
};

template<typename command>
marshall& operator<<(marshall &m, const append_entries_args<command>& args) {
    // Your code here
    m<<args.term;
    m<<args.leader_id;
    m<<args.prev_log_index;
    m<<args.prev_log_term;
    m<<args.entries;
    m<<args.leader_commit;
    return m;
}

template<typename command>
unmarshall& operator>>(unmarshall &u, append_entries_args<command>& args) {
    // Your code here
    u>>args.term;
    u>>args.leader_id;
    u>>args.prev_log_index;
    u>>args.prev_log_term;
    u>>args.entries;
    u>>args.leader_commit;
    return u;
}

class append_entries_reply {
public:
    // Your code here
    int term;
    bool success;
    append_entries_reply(int term_, bool success_):term(term_),success(success_){}
    append_entries_reply()=default;
};

marshall& operator<<(marshall &m, const append_entries_reply& reply);
unmarshall& operator>>(unmarshall &m, append_entries_reply& reply);


class install_snapshot_args {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_args& args);
unmarshall& operator>>(unmarshall &m, install_snapshot_args& args);


class install_snapshot_reply {
public:
    // Your code here
};

marshall& operator<<(marshall &m, const install_snapshot_reply& reply);
unmarshall& operator>>(unmarshall &m, install_snapshot_reply& reply);


#endif // raft_protocol_h