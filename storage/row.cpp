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

#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_occ.h"
#include "row_maat.h"
#include "row_wkdb.h"
#include "mem_alloc.h"
#include "manager.h"

#define SIM_FULL_ROW true

RC 
row_t::init(table_t * host_table, uint64_t part_id, uint64_t row_id) {
	part_info = true;
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;
	Catalog * schema = host_table->get_schema();
	tuple_size = schema->get_tuple_size();
#if SIM_FULL_ROW
	data = (char *) mem_allocator.alloc(sizeof(char) * tuple_size);
#else
	data = (char *) mem_allocator.alloc(sizeof(uint64_t) * 1);
#endif
	return RCOK;
}

RC 
row_t::switch_schema(table_t * host_table) {
	this->table = host_table;
	return RCOK;
}

void row_t::init_manager(row_t * row) {
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
  return;
#endif
  DEBUG_M("row_t::init_manager alloc \n");
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == CALVIN
    manager = (Row_lock *) mem_allocator.align_alloc(sizeof(Row_lock));
#elif CC_ALG == TIMESTAMP
    manager = (Row_ts *) mem_allocator.align_alloc(sizeof(Row_ts));
#elif CC_ALG == MVCC
    manager = (Row_mvcc *) mem_allocator.align_alloc(sizeof(Row_mvcc));
#elif CC_ALG == OCC
    manager = (Row_occ *) mem_allocator.align_alloc(sizeof(Row_occ));
#elif CC_ALG == MAAT 
    manager = (Row_maat *) mem_allocator.align_alloc(sizeof(Row_maat));
#elif CC_ALG == WOOKONG 
    manager = (Row_wkdb *) mem_allocator.align_alloc(sizeof(Row_wkdb));
#endif

#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC 
	manager->init(this);
#endif
}

table_t * row_t::get_table() { 
	return table; 
}

Catalog * row_t::get_schema() { 
	return get_table()->get_schema(); 
}

const char * row_t::get_table_name() { 
	return get_table()->get_table_name(); 
};
uint64_t row_t::get_tuple_size() {
	return get_schema()->get_tuple_size();
}

uint64_t row_t::get_field_cnt() { 
	return get_schema()->field_cnt;
}

void row_t::set_value(int id, void * ptr) {
	int datasize = get_schema()->get_field_size(id);
	int pos = get_schema()->get_field_index(id);
  DEBUG("set_value pos %d datasize %d -- %lx\n",pos,datasize,(uint64_t)this);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, datasize);
#else
  char d[tuple_size];
	memcpy( &d[pos], ptr, datasize);
#endif
}

void row_t::set_value(int id, void * ptr, int size) {
	int pos = get_schema()->get_field_index(id);
#if SIM_FULL_ROW
	memcpy( &data[pos], ptr, size);
#else
  char d[tuple_size];
	memcpy( &d[pos], ptr, size);
#endif
}

void row_t::set_value(const char * col_name, void * ptr) {
	uint64_t id = get_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char * row_t::get_value(int id) {
  int pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(id);
  DEBUG("get_value pos %d -- %lx\n",pos,(uint64_t)this);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * row_t::get_value(char * col_name) {
  uint64_t pos __attribute__ ((unused));
	pos = get_schema()->get_field_index(col_name);
#if SIM_FULL_ROW
	return &data[pos];
#else
	return data;
#endif
}

char * row_t::get_data() { 
  return data; 
}

void row_t::set_data(char * data) { 
	int tuple_size = get_schema()->get_tuple_size();
#if SIM_FULL_ROW
	memcpy(this->data, data, tuple_size);
#else
  char d[tuple_size];
	memcpy(d, data, tuple_size);
#endif
}
// copy from the src to this
void row_t::copy(row_t * src) {
	assert(src->get_schema() == this->get_schema());
#if SIM_FULL_ROW
	set_data(src->get_data());
#else
  char d[tuple_size];
	set_data(d);
#endif
}

void row_t::free_row() {
  DEBUG_M("row_t::free_row free\n");
#if SIM_FULL
	mem_allocator.free(data, sizeof(char) * get_tuple_size());
#else
	mem_allocator.free(data, sizeof(uint64_t) * 1);
#endif
}

RC row_t::get_lock(access_t type, TxnManager * txn) {
  RC rc = RCOK;
#if CC_ALG == CALVIN
	lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
	rc = this->manager->lock_get(lt, txn);
#endif
  return rc;
}

RC row_t::get_row(access_t type, TxnManager * txn, row_t *& row) {
    RC rc = RCOK;
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE 
    row = this;
    return rc;
#endif
#if ISOLATION_LEVEL == NOLOCK
    row = this;
    return rc;
#endif
  /*
#if ISOLATION_LEVEL == READ_UNCOMMITTED
  if(type == RD) {
    row = this;
    return rc;
  }
#endif
*/
#if CC_ALG == MAAT

    DEBUG_M("row_t::get_row MAAT alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
    rc = this->manager->access(type,txn);
    txn->cur_row->copy(this);
	row = txn->cur_row;
    assert(rc == RCOK);
	goto end;
#endif
#if CC_ALG == WOOKONG
	if (type == WR) {
		rc = this->manager->access(P_REQ, txn, NULL);
		if (rc != RCOK) 
			goto end;
	}
	if ((type == WR && rc == RCOK) || type == RD || type == SCAN) {
		rc = this->manager->access(R_REQ, txn, NULL);
		if (rc == RCOK ) {
			row = txn->cur_row;
		} else if (rc == WAIT) {
		      rc = WAIT;
		      goto end;

		} else if (rc == Abort) {

		}
        if (rc != Abort) {
            assert(row->get_data() != NULL);
            assert(row->get_table() != NULL);
            assert(row->get_schema() == this->get_schema());
            assert(row->get_table_name() != NULL);
        }
	}

	if (rc != Abort && type == WR) {
	    DEBUG_M("row_t::get_row MVCC alloc \n");
		row_t * newr = (row_t *) mem_allocator.alloc(sizeof(row_t));
		newr->init(this->get_table(), get_part_id());
		newr->copy(row);
		row = newr;
	}
	goto end;

#endif
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT 
	//uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
	rc = this->manager->lock_get(lt, txn);

	if (rc == RCOK) {
		row = this;
	} else if (rc == Abort) {} 
	else if (rc == WAIT) {
		ASSERT(CC_ALG == WAIT_DIE);

	}
	goto end;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
	//uint64_t thd_id = txn->get_thd_id();
	// For TIMESTAMP RD, a new copy of the row will be returned.
	// for MVCC RD, the version will be returned instead of a copy
	// So for MVCC RD-WR, the version should be explicitly copied.
	// row_t * newr = NULL;
#if CC_ALG == TIMESTAMP
	// TIMESTAMP makes a whole copy of the row before reading
  DEBUG_M("row_t::get_row TIMESTAMP alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), this->get_part_id());
#endif
	
	if (type == WR) {
		rc = this->manager->access(txn, P_REQ, NULL);
		if (rc != RCOK) 
			goto end;
	}
	if ((type == WR && rc == RCOK) || type == RD || type == SCAN) {
		rc = this->manager->access(txn, R_REQ, NULL);
		if (rc == RCOK ) {
			row = txn->cur_row;
		} else if (rc == WAIT) {
		      rc = WAIT;
		      goto end;

		} else if (rc == Abort) {

		}
        if (rc != Abort) {
            assert(row->get_data() != NULL);
            assert(row->get_table() != NULL);
            assert(row->get_schema() == this->get_schema());
            assert(row->get_table_name() != NULL);
        }
	}
	if (rc != Abort &&( CC_ALG == MVCC) && type == WR) {
	    DEBUG_M("row_t::get_row MVCC alloc \n");
		row_t * newr = (row_t *) mem_allocator.alloc(sizeof(row_t));
		newr->init(this->get_table(), get_part_id());
		newr->copy(row);
		row = newr;
	}
	goto end;
#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write
  DEBUG_M("row_t::get_row OCC alloc \n");
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	txn->cur_row->init(get_table(), get_part_id());
	rc = this->manager->access(txn, R_REQ);
	row = txn->cur_row;
	goto end;
#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN
#if CC_ALG == HSTORE_SPEC
  if(txn_table.spec_mode) {
    DEBUG_M("row_t::get_row HSTORE_SPEC alloc \n");
	  txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t));
	  txn->cur_row->init(get_table(), get_part_id());
	  rc = this->manager->access(txn, R_REQ);
	  row = txn->cur_row;
	  goto end;
  }
#endif
	row = this;
	goto end;
#else
	assert(false);
#endif

end:
  return rc;
}

// Return call for get_row if waiting 
RC row_t::get_row_post_wait(access_t type, TxnManager * txn, row_t *& row) {

  RC rc = RCOK;
  assert(CC_ALG == WAIT_DIE || CC_ALG == MVCC || CC_ALG == WOOKONG || CC_ALG == TIMESTAMP);
#if CC_ALG == WAIT_DIE
  assert(txn->lock_ready);
	rc = RCOK;
	//ts_t endtime = get_sys_clock();
	row = this;

#elif CC_ALG == MVCC || CC_ALG == TIMESTAMP || CC_ALG == WOOKONG
			assert(txn->ts_ready);
			//INC_STATS(thd_id, time_wait, t2 - t1);
			row = txn->cur_row;

			assert(row->get_data() != NULL);
			assert(row->get_table() != NULL);
			assert(row->get_schema() == this->get_schema());
			assert(row->get_table_name() != NULL);
	if (( CC_ALG == MVCC || CC_ALG == WOOKONG) && type == WR) {
    DEBUG_M("row_t::get_row_post_wait MVCC alloc \n");
		row_t * newr = (row_t *) mem_allocator.alloc(sizeof(row_t));
		newr->init(this->get_table(), get_part_id());

		newr->copy(row);
		row = newr;
	}
#endif
  return rc;
}

// the "row" is the row read out in get_row(). For locking based CC_ALG, 
// the "row" is the same as "this". For timestamp based CC_ALG, the 
// "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be 
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (c.f. row_ts.cpp)
void row_t::return_row(RC rc, access_t type, TxnManager * txn, row_t * row) {	
#if MODE==NOCC_MODE || MODE==QRY_ONLY_MODE
  return;
#endif
#if ISOLATION_LEVEL == NOLOCK
  return;
#endif
  /*
#if ISOLATION_LEVEL == READ_UNCOMMITTED
  if(type == RD) {
    return;
  }
#endif
*/
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == CALVIN
	assert (row == NULL || row == this || type == XP);
	if (CC_ALG != CALVIN && ROLL_BACK && type == XP) {// recover from previous writes. should not happen w/ Calvin
		this->copy(row);
	}
	this->manager->lock_release(txn);
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
	// for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free. 
	if (CC_ALG == TIMESTAMP && (type == RD || type == SCAN)) {
		row->free_row();
    DEBUG_M("row_t::return_row TIMESTAMP free \n");
		mem_allocator.free(row, sizeof(row_t));
	}
	if (type == XP) {
		row->free_row();
    DEBUG_M("row_t::return_row XP free \n");
		mem_allocator.free(row, sizeof(row_t));
		this->manager->access(txn, XP_REQ, NULL);
	} else if (type == WR) {
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
		RC rc = this->manager->access(txn, W_REQ, row);
		assert(rc == RCOK);
	}
#elif CC_ALG == OCC
	assert (row != NULL);
	if (type == WR)
		manager->write( row, txn->get_end_timestamp() );
	row->free_row();
  DEBUG_M("row_t::return_row OCC free \n");
	mem_allocator.free(row, sizeof(row_t));
  manager->release();
	return;
#elif CC_ALG == MAAT
	assert (row != NULL);
  if (rc == Abort) {
    manager->abort(type,txn);
  } else {
    manager->commit(type,txn,row);
  }

	row->free_row();
  DEBUG_M("row_t::return_row Maat free \n");
	mem_allocator.free(row, sizeof(row_t));
#elif CC_ALG == WOOKONG
	assert (row != NULL);

	if (rc == Abort) {
		manager->abort(type,txn);
	} else {
		manager->commit(type,txn,row);
	}

	if (type == XP) {
		row->free_row();
    	DEBUG_M("row_t::return_row XP free \n");
		mem_allocator.free(row, sizeof(row_t));
	}

#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC 
	assert (row != NULL);
	if (ROLL_BACK && type == XP) {// recover from previous writes.
		this->copy(row);
	}
	return;
#else 
	assert(false);
#endif
}

