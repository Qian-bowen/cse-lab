#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		// Lab2: Your definition here.
		uint32_t task_type;
		int task_num;
		int index;//for reduce means partition,for map means file index
		string filename;//for map
	};

	// struct AskTaskRequest {
	// 	// Lab2: Your definition here.
	// };

	// struct SubmitTaskResponse {
	// 	// Lab2: Your definition here.
	// };

	// struct SubmitTaskRequest {
	// 	// Lab2: Your definition here.
	// };

};

inline unmarshall &
operator>>(unmarshall &u, mr_protocol::AskTaskResponse &a)
{
	u >> a.task_type;
	u >> a.task_num;
	u >> a.index;
	u >> a.filename;
	return u;
}

inline marshall &
operator<<(marshall &m, mr_protocol::AskTaskResponse a)
{
	m << a.task_type;
	m << a.task_num;
  	m << a.index;
  	m << a.filename;
  	return m;
}

#endif

