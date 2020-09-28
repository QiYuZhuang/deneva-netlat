/*
	 Copyright 2016 Massachusetts Institute of Technology

	 Licensed under the Apache License, Version 2.0 (the "License");
	 you may not use this file except in compliance with the License.
	 You may obtain a copy of the License at

			 http://www.apache.org/licenses/LICENSE-2.0

	 Unless required by applicable law or agreed to in writing, software
	 distributed under the License is distributed on an "AS IS" BASIS,
	 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 See the License for the specific language governing permissions and
	 limitations under the License.
*/

#ifndef _TXN_H_
#define _TXN_H_

#include "global.h"
#include "helper.h"
#include "semaphore.h"
#include "array.h"
#include "transport/message.h"
//#include "wl.h"

class Workload;
class Thread;
class row_t;
class table_t;
class BaseQuery;
class INDEX;
class TxnQEntry; 
class YCSBQuery;
class TPCCQuery;
//class r_query;

enum TxnState {START,INIT,EXEC,PREP,FIN,DONE};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	row_t * 	data;
	row_t * 	orig_data;
	uint64_t version;
#if CC_ALG == TICTOC
	uint64_t    orig_wts;
	uint64_t    orig_rts;
	bool         locked;
#endif
	void cleanup();
};

class Transaction {
public:
	void init();
	void reset(uint64_t thd_id);
	void release_accesses(uint64_t thd_id);
	void release_inserts(uint64_t thd_id);
	void release(uint64_t thd_id);
	//vector<Access*> accesses;
	Array<Access*> accesses;
	uint64_t timestamp;
	// For OCC and SSI
	uint64_t start_timestamp;
	uint64_t end_timestamp;

	uint64_t write_cnt;
	uint64_t row_cnt;
	// Internal state
	TxnState twopc_state;
	Array<row_t*> insert_rows;
	txnid_t         txn_id;
	uint64_t batch_id;
	RC rc;
};

class TxnStats {
public:
	void init();
	void clear_short();
	void reset();
	void abort_stats(uint64_t thd_id);
	void commit_stats(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id, uint64_t timespan_long,
										uint64_t timespan_short);
	uint64_t starttime;
	uint64_t restart_starttime;
	uint64_t wait_starttime;
	uint64_t write_cnt;
	uint64_t abort_cnt;
	double total_process_time;
	double process_time;
	double total_local_wait_time;
	double local_wait_time;
	double total_remote_wait_time;  // time waiting for a remote response, to help calculate network
																	// time
	double remote_wait_time;
	double total_twopc_time;
	double twopc_time;
	double total_abort_time; // time spent in aborted query land
	double total_msg_queue_time; // time spent on outgoing queue
	double msg_queue_time;
	double total_work_queue_time; // time spent on work queue
	double work_queue_time;
	double total_cc_block_time; // time spent blocking on a cc resource
	double cc_block_time;
	double total_cc_time; // time spent actively doing cc
	double cc_time;
	uint64_t total_work_queue_cnt;
	uint64_t work_queue_cnt;

	// short stats
	double work_queue_time_short;
	double cc_block_time_short;
	double cc_time_short;
	double msg_queue_time_short;
	double process_time_short;
	double network_time_short;

	double lat_network_time_start;
	double lat_other_time_start;
};

/*
	 Execution of transactions
	 Manipulates/manages Transaction (contains txn-specific data)
	 Maintains BaseQuery (contains input args, info about query)
	 */
class TxnManager {
public:
	virtual ~TxnManager() {}
	virtual void init(uint64_t thd_id,Workload * h_wl);
	virtual void reset();
	void clear();
	void reset_query();
	void release();
	Thread * h_thd;
	Workload * h_wl;

	virtual RC      run_txn() = 0;
	virtual RC      run_txn_post_wait() = 0;
	virtual RC      run_calvin_txn() = 0;
	virtual RC      acquire_locks() = 0;
	void            register_thread(Thread * h_thd);
	uint64_t        get_thd_id();
	Workload *      get_wl();
	void            set_txn_id(txnid_t txn_id);
	txnid_t         get_txn_id();
	void            set_query(BaseQuery * qry);
	BaseQuery *     get_query();
	bool            is_done();
	void            commit_stats();
	bool            is_multi_part();

	void            set_timestamp(ts_t timestamp);
	ts_t            get_timestamp();
	void            set_start_timestamp(uint64_t start_timestamp);
	ts_t            get_start_timestamp();
	uint64_t        get_rsp_cnt() {return rsp_cnt;}
	uint64_t        incr_rsp(int i);
	uint64_t        decr_rsp(int i);
	uint64_t        incr_lr();
	uint64_t        decr_lr();

	RC commit();
	RC start_commit();
	RC start_abort();
	RC abort();

	void release_locks(RC rc);
	bool isRecon() {
		assert(CC_ALG == CALVIN || !recon);
		return recon;
	};
		bool recon;

	row_t * volatile cur_row;
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	int volatile   lock_ready;
	// [TIMESTAMP, MVCC]
	bool volatile   ts_ready;
	// [HSTORE, HSTORE_SPEC]
	int volatile    ready_part;
	int volatile    ready_ulk;
	bool aborted;
	uint64_t return_id;
	RC        validate();
	void            cleanup(RC rc);
	void            cleanup_row(RC rc,uint64_t rid);
	void release_last_row_lock();
	RC send_remote_reads();
	void set_end_timestamp(uint64_t timestamp) {txn->end_timestamp = timestamp;}
	uint64_t get_end_timestamp() {return txn->end_timestamp;}
	uint64_t get_access_cnt() {return txn->row_cnt;}
	uint64_t get_write_set_size() {return txn->write_cnt;}
	uint64_t get_read_set_size() {return txn->row_cnt - txn->write_cnt;}
	access_t get_access_type(uint64_t access_id) {return txn->accesses[access_id]->type;}
	uint64_t get_access_version(uint64_t access_id) { return txn->accesses[access_id]->version; }
	row_t * get_access_original_row(uint64_t access_id) {return txn->accesses[access_id]->orig_row;}
	void swap_accesses(uint64_t a, uint64_t b) { txn->accesses.swap(a, b); }
	uint64_t get_batch_id() {return txn->batch_id;}
	void set_batch_id(uint64_t batch_id) {txn->batch_id = batch_id;}

		// For MaaT
	uint64_t commit_timestamp;
	uint64_t get_commit_timestamp() {return commit_timestamp;}
	void set_commit_timestamp(uint64_t timestamp) {commit_timestamp = timestamp;}
	uint64_t greatest_write_timestamp;
	uint64_t greatest_read_timestamp;
	std::set<uint64_t> * uncommitted_reads;
	std::set<uint64_t> * uncommitted_writes;
	std::set<uint64_t> * uncommitted_writes_y;
	
	uint64_t twopl_wait_start;

	// For Tictoc
	uint64_t _min_commit_ts;
	uint64_t _max_commit_ts;
	volatile uint32_t _num_lock_waits;
	bool _signal_abort;
	bool _is_sub_txn;

	uint64_t _timestamp;
	uint64_t     get_priority() { return _timestamp; }
	// debug time
	uint64_t _start_wait_time;
	uint64_t _lock_acquire_time;
	uint64_t _lock_acquire_time_commit;
	uint64_t _lock_acquire_time_abort;
	////////////////////////////////
	// LOGGING
	////////////////////////////////
//	void 			gen_log_entry(int &length, void * log);
	bool log_flushed;
	bool repl_finished;
	Transaction * txn;
	BaseQuery * query;
	uint64_t client_startts;
	uint64_t client_id;
	uint64_t get_abort_cnt() {return abort_cnt;}
	uint64_t abort_cnt;
	int received_response(RC rc);
	bool waiting_for_response();
	RC get_rc() {return txn->rc;}
	void set_rc(RC rc) {txn->rc = rc;}
	//void send_rfin_messages(RC rc) {assert(false);}
	void send_finish_messages();
	void send_prepare_messages();

	TxnStats txn_stats;

	bool set_ready() {return ATOM_CAS(txn_ready,0,1);}
	bool unset_ready() {return ATOM_CAS(txn_ready,1,0);}
	bool is_ready() {return txn_ready == true;}
	volatile int txn_ready;
	// Calvin
	uint32_t lock_ready_cnt;
	uint32_t calvin_expected_rsp_cnt;
	bool locking_done;
	CALVIN_PHASE phase;
	Array<row_t*> calvin_locked_rows;
	bool calvin_exec_phase_done();
	bool calvin_collect_phase_done();

	int last_batch_id;
	int last_txn_id;
	Message* last_msg;

#if CC_ALG == DLI_MVCC || CC_ALG == DLI_MVCC_OCC || CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3 || \
	CC_ALG == DLI_OCC
	std::atomic<bool>* is_abort = nullptr;
#endif

protected:

	int rsp_cnt;
	void            insert_row(row_t * row, table_t * table);

	itemid_t *      index_read(INDEX * index, idx_key_t key, int part_id);
	itemid_t *      index_read(INDEX * index, idx_key_t key, int part_id, int count);
	RC get_lock(row_t * row, access_t type);
	RC get_row(row_t * row, access_t type, row_t *& row_rtn);
	RC get_row_post_wait(row_t *& row_rtn);

	// For Waiting
	row_t * last_row;
	row_t * last_row_rtn;
	access_t last_type;

	sem_t rsp_mutex;
	bool registed_;
};

#endif

