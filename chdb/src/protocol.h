#ifndef protocal_h
#define protocal_h

#include "rpc.h"

namespace chdb_protocol {
    enum rpc_numbers {
        Dummy = 0xdead,
        /* KV storage related */
        Put,
        Get,
        /* Transaction related */
        Prepare,
        CheckPrepareState,
        Commit,
        Rollback,
    };

    /* Used for 2PC protocol */
    enum prepare_state {
        prepare_not_ok = 0,
        prepare_ok,
    };

    class operation_var {
    public:
        int tx_id;
        int key;
        int value;

        operation_var(){}
        operation_var(int tx_id_,int key_,int value_):tx_id(tx_id_),key(key_),value(value_){}
    };

    class dummy_var {
    public:
        int v0;
        int v1;
    };

    class prepare_var {
    public:
        int tx_id;
        prepare_var(){}
        prepare_var(int tx_id_):tx_id(tx_id_){}
    };

    class check_prepare_state_var {
    public:
        int tx_id;

        check_prepare_state_var(){}
        check_prepare_state_var(int tx_id_):tx_id(tx_id_){}
    };

    class commit_var {
    public:
        int tx_id;

        commit_var(){}
        commit_var(int tx_id_):tx_id(tx_id_){}
    };

    class rollback_var {
    public:
        int tx_id;

        rollback_var(){}
        rollback_var(int tx_id_):tx_id(tx_id_){}
    };

    marshall &
    operator<<(marshall &m, const dummy_var &var);

    unmarshall &
    operator>>(unmarshall &u, dummy_var &var);

    marshall &
    operator<<(marshall &m, const operation_var &var);

    unmarshall &
    operator>>(unmarshall &u, operation_var &var);

    marshall &
    operator<<(marshall &m, const prepare_var &var);

    unmarshall &
    operator>>(unmarshall &u, prepare_var &var);

    marshall &
    operator<<(marshall &m, const check_prepare_state_var &var);

    unmarshall &
    operator>>(unmarshall &u, check_prepare_state_var &var);

    marshall &
    operator<<(marshall &m, const commit_var &var);

    unmarshall &
    operator>>(unmarshall &u, commit_var &var);

    marshall &
    operator<<(marshall &m, const rollback_var &var);

    unmarshall &
    operator>>(unmarshall &u, rollback_var &var);
}


#endif protocal_h
