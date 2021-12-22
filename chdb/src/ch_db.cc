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