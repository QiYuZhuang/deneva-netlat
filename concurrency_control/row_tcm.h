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

#ifndef ROW_TCM_H
#define ROW_TCM_H

#include "row_lock.h"

class Row_tcm {
public:
    void init(row_t *row);
    // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, TxnManager *txn);
    RC lock_get(lock_t type, TxnManager *txn, uint64_t *&txnids, int &txncnt);
    RC lock_release(TxnManager *txn);

private:
    pthread_mutex_t *latch;
    bool blatch;
    void push_lock_stack(TxnManager *txn, lock_t type);
    bool can_be_before(TxnManager *former, TxnManager *later);
    void put_before(TxnManager *former, TxnManager *later, bool later_is_reader);
    RC add_reader_entry_into_waiters(TxnManager *txn);
    RC add_writer_entry_into_waiters(TxnManager *txn);
    RC adjust_timestamps_rw(TxnManager *holder, TxnManager *requester);
    RC adjust_timestamps_wr(TxnManager *holder, TxnManager *requester);
    RC adjust_timestamps_ww(TxnManager *holder, TxnManager *requester);
    void lock_ts_interval(TxnManager *txn1, TxnManager *txn2);
    void unlock_ts_interval(TxnManager *txn1, TxnManager *txn2);
    bool conflict_lock(lock_t l1, lock_t l2);
    LockEntry *get_entry();
    void return_entry(LockEntry *entry);
    row_t *_row;
    uint64_t hash(uint64_t id) { return id % owners_size; };
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;

    // owners is a hash table
    // waiters is a double linked list
    // [waiters] head is the oldest txn, tail is the youngest txn.
    //   So new txns are inserted into the tail.
    LockEntry **owners;
    uint64_t owners_size;
    LockEntry *waiters_head;
    LockEntry *waiters_tail;
    uint64_t max_owner_ts;
    uint64_t own_starttime;

    // For TCM
    uint64_t last_committed_read_ts;
    uint64_t last_committed_write_ts;
};

#endif
