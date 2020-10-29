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

#ifndef ROW_SSI_H
#define ROW_SSI_H

class table_t;
class Catalog;
class TxnManager;

struct SSIReqEntry {
	TxnManager * txn;
	ts_t ts;
	ts_t starttime;
	SSIReqEntry * next;
};

struct SSIHisEntry {
	// TxnManager * txn;
	txnid_t txn;
	ts_t ts;
	// only for write history. The value needs to be stored.
//	char * data;
	row_t * row;
	SSIHisEntry * next;
	SSIHisEntry * prev;
};

struct SSILockEntry {
    lock_t type;
    ts_t   start_ts;
    // TxnManager * txn;
	txnid_t txn;
	SSILockEntry * next;
	SSILockEntry * prev;
};

class Row_ssi {
public:
	void init(row_t * row);
	RC access(TxnManager * txn, TsType type, row_t * row);
private:
 	pthread_mutex_t * latch;

	SSILockEntry * si_read_lock;
	SSILockEntry * write_lock;

	bool blatch;

	row_t * _row;
	void get_lock(lock_t type, TxnManager * txn);
	void release_lock(lock_t type, TxnManager * txn);

	void insert_history(ts_t ts, TxnManager * txn, row_t * row);

	SSIReqEntry * get_req_entry();
	void return_req_entry(SSIReqEntry * entry);
	SSIHisEntry * get_his_entry();
	void return_his_entry(SSIHisEntry * entry);

	bool conflict(TsType type, ts_t ts);
	void buffer_req(TsType type, TxnManager * txn);
	SSIReqEntry * debuffer_req( TsType type, TxnManager * txn = NULL);


	SSILockEntry * get_entry();
	row_t * clear_history(TsType type, ts_t ts);

    SSIReqEntry * prereq_mvcc;
    SSIHisEntry * readhis;
    SSIHisEntry * writehis;
	SSIHisEntry * readhistail;
	SSIHisEntry * writehistail;

	uint64_t whis_len;
	uint64_t rhis_len;
	uint64_t preq_len;
};

#endif
