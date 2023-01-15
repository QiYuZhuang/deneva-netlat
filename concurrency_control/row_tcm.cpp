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

#include "row.h"
#include "txn.h"
#include "table.h"
#include "row_tcm.h"
#include "mem_alloc.h"
#include "manager.h"
#include "helper.h"

void Row_tcm::init(row_t *row) {
    _row = row;
    owners_size = 1;//1031;
    owners = NULL;
    owners = (LockEntry**) mem_allocator.alloc(sizeof(LockEntry*)*owners_size);
    for(uint64_t i = 0; i < owners_size; i++)
        owners[i] = NULL;
    waiters_head = NULL;
    waiters_tail = NULL;
    owner_cnt = 0;
    waiter_cnt = 0;
    max_owner_ts = 0;

    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);

    lock_type = LOCK_NONE;
    blatch = false;
    own_starttime = 0;
}

RC Row_tcm::lock_get(lock_t type, TxnManager * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_tcm::lock_get(lock_t type, TxnManager * txn, uint64_t* &txnids, int &txncnt) {
    assert(CC_ALG == TCM);
    // printf("lock_get begin, txn_id: %lu\n", txn->get_txn_id());
    RC rc;
    bool can_adjust_1 = true; // can_be_before(requestor, holder), requestor is reader
    bool can_adjust_2 = true; // can_be_before(holder, requestor), requester is reader or writer

    LockEntry *en;
    uint64_t starttime = get_sys_clock();

    if (g_central_man) {
        glob_manager.lock_row(_row);
    }
    else {
        uint64_t mtx_wait_starttime = get_sys_clock();
        pthread_mutex_lock( latch );
        INC_STATS(txn->get_thd_id(),mtx[17],get_sys_clock() - mtx_wait_starttime);
    }

    if(owner_cnt > 0) {
        INC_STATS(txn->get_thd_id(),twopl_already_owned_cnt,1);
    }
    
    bool conflict = conflict_lock(lock_type, type);

    pthread_mutex_lock(txn->ts_interval_lock);
    if (txn->get_tcm_early() > txn->get_tcm_late()) {
        pthread_mutex_unlock(txn->ts_interval_lock);
        rc = Abort;
        goto final;
    }
    pthread_mutex_unlock(txn->ts_interval_lock);

    if (!conflict) {
        DEBUG("tcm lock (%ld,%ld): owners %d, own type %d, req type %d, key %ld %lx\n",txn->get_txn_id(),txn->get_batch_id(),owner_cnt,lock_type,type,_row->get_primary_key(),(uint64_t)_row);
        if(owner_cnt > 0) {
            assert(type == LOCK_SH);
            INC_STATS(txn->get_thd_id(),twopl_sh_bypass_cnt,1);
        }

        push_lock_stack(txn, type); // grant lock

        if(lock_type == LOCK_NONE) {
            own_starttime = get_sys_clock();
        }
        lock_type = type;
        rc = RCOK;
        goto final;
    }
    pthread_mutex_lock(txn->ts_interval_lock);
    if (type == LOCK_SH) {
        if (txn->get_tcm_end() && last_committed_write_ts + 1 >= txn->get_tcm_late()) {
            pthread_mutex_unlock(txn->ts_interval_lock);
            rc = Abort;
            goto final;
        } else {
            uint64_t ts_committed = last_committed_write_ts;
            txn->set_tcm_early(max(ts_committed + 1, txn->get_tcm_early()));
        }
    } else {
        if ((txn->get_tcm_end() && last_committed_write_ts + 1 >= txn->get_tcm_late()) || 
            (txn->get_tcm_end() && last_committed_read_ts + 1 >= txn->get_tcm_late())) {
            pthread_mutex_unlock(txn->ts_interval_lock);
            rc = Abort;
            goto final;
        } else {
            uint64_t ts_committed = max(last_committed_read_ts, last_committed_write_ts);
            txn->set_tcm_early(max(ts_committed + 1, txn->get_tcm_early()));
        }
    }
    

    if (txn->get_tcm_end() && txn->get_tcm_early() >= txn->get_tcm_late()) {
        pthread_mutex_unlock(txn->ts_interval_lock);
        rc = Abort;
        goto final;
    }
    pthread_mutex_unlock(txn->ts_interval_lock);

    // find conflict, check timestamp interval if need block
    /* owners can one writer and some readers */ 
    for(uint64_t i = 0; i < owners_size; i++) {
        en = owners[i];
        while (en != NULL) {
            assert(txn->get_txn_id() != en->txn->get_txn_id());
            // assert(txn->get_timestamp() != en->txn->get_timestamp());

            // if (can_adjust_1 && en->type == LOCK_EX && !can_be_before(txn, en->txn)) // after LOCK_EX 
            if (can_adjust_1 && !can_be_before(txn, en->txn)) // after LOCK_EX 
                can_adjust_1 = false;

            if (can_adjust_2 && !can_be_before(en->txn, txn)) 
                can_adjust_2 = false;
            
            if (!can_adjust_1 && !can_adjust_2)
                break;

            en = en->next;
        }

        if(!can_adjust_1 && !can_adjust_2)
            break;
    }

    if (type == LOCK_SH) { // requestor is reader
        // always success
        if (can_adjust_1) {
            for(uint64_t i = 0; i < owners_size; i++) {
                en = owners[i];
                while (en != NULL) {
                    if (en->type == LOCK_EX) {
                        lock_ts_interval(txn, en->txn);
                        if (!can_be_before(txn, en->txn)) {
                            printf("txn_id: %lu, line 155.\n", txn->get_txn_id());
                            unlock_ts_interval(txn, en->txn);
                            if (can_adjust_2) {
                                goto adjust_2;
                            }
                            rc = Abort;
                            goto final;
                        }
                        put_before(txn, en->txn, false);
                        unlock_ts_interval(txn, en->txn);
                    }
                    en = en->next;
                }
            }
            // if (txn->get_tcm_early() > txn->get_tcm_late()) {
            //     rc = Abort;
            //     goto final;
            // }
            push_lock_stack(txn, type);
            rc = RCOK;
            goto final;
        }

        // block 
adjust_2:
        if (can_adjust_2) {
            for(uint64_t i = 0; i < owners_size; i++) {
                en = owners[i];
                while (en != NULL) {
                    lock_ts_interval(txn, en->txn);
                    if (!can_be_before(en->txn, txn)) {
                        printf("txn_id: %lu, line 177.\n", txn->get_txn_id());
                        unlock_ts_interval(txn, en->txn);
                        rc = Abort;
                        goto final;
                    }
                    put_before(en->txn, txn, true);
                    unlock_ts_interval(txn, en->txn);
                    en = en->next;
                }
            }
            rc = add_reader_entry_into_waiters(txn); // WAIT or Abort
            goto final;
        }
        
        rc = Abort;
        goto final;
    } else if (type == LOCK_EX) { // requestor is writer
        /*
         * writer can meet conflicts in 2 situations:
         * 1. block by writer, maybe some readers, find the writer
         * 2. block by readers
         */ 
        bool block_by_reader = lock_type == LOCK_SH;
        if (can_adjust_2) {
            if (block_by_reader) {
                for(uint64_t i = 0; i < owners_size; i++) {
                    en = owners[i];
                    while (en != NULL) {
                        lock_ts_interval(txn, en->txn);
                        if (!can_be_before(en->txn, txn)) {
                            printf("txn_id: %lu, line 202.\n", txn->get_txn_id());
                            unlock_ts_interval(txn, en->txn);
                            rc = Abort;
                            goto final;
                        }
                        put_before(en->txn, txn, false);
                        unlock_ts_interval(txn, en->txn);
                        en = en->next;
                    }
                }

                // adjust timestamp interval, put txn before waiters
                if (waiter_cnt > 0) {
                    en = waiters_head;
                    printf("it is weird");
                    while(en != nullptr) {
                        lock_ts_interval(txn, en->txn);
                        if (can_be_before(txn, en->txn)) {
                            put_before(txn, en->txn, en->type == LOCK_SH);
                            unlock_ts_interval(txn, en->txn);
                        } else {
                            unlock_ts_interval(txn, en->txn);
                            rc = Abort;
                            goto final;
                        }
                        en = en->next;
                    }
                }
                // if (txn->get_tcm_early() > txn->get_tcm_late()) {
                //     rc = Abort;
                //     goto final;
                // }
                push_lock_stack(txn, type);
                lock_type = LOCK_EX; // change lock type
                rc = RCOK;
                goto final;
            } else {
                for(uint64_t i = 0; i < owners_size; i++) {
                    en = owners[i];
                    while (en != NULL) {
                        lock_ts_interval(txn, en->txn);
                        if (!can_be_before(en->txn, txn)) {
                            printf("txn_id: %lu, line 237.\n", txn->get_txn_id());
                            unlock_ts_interval(txn, en->txn);
                            rc = Abort;
                            goto final;
                        }
                        put_before(en->txn, txn, false);
                        unlock_ts_interval(txn, en->txn);
                        en = en->next;
                    }
                }
                // add into wait-list
                rc = add_writer_entry_into_waiters(txn);

                goto final;
            }
        }
        rc = Abort;
        goto final;
    } else {
        assert(type != LOCK_EX && type != LOCK_SH);
    }

final:
    assert(txn->get_tcm_early() <= txn->get_tcm_late());
    uint64_t curr_time = get_sys_clock();
    uint64_t timespan = curr_time - starttime;
    if (rc == WAIT && txn->twopl_wait_start == 0) {
        txn->twopl_wait_start = curr_time;
    }
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(),twopl_getlock_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_getlock_cnt,1);
        
    if (g_central_man)
        glob_manager.release_row(_row);
    else
        pthread_mutex_unlock(latch);

    // printf("lock_get end, txn_id: %lu,lock\n", txn->get_txn_id());
    return rc;
}

RC Row_tcm::lock_release(TxnManager *txn) {
    assert(txn != nullptr);
    // printf("lock_release begin, txn_id: %lu\n", txn->get_txn_id());

    RC rc = RCOK;	
    uint64_t starttime = get_sys_clock();
    if (g_central_man)
        glob_manager.lock_row(_row);
    else {
        uint64_t mtx_wait_starttime = get_sys_clock();
        pthread_mutex_lock( latch );
        INC_STATS(txn->get_thd_id(),mtx[18],get_sys_clock() - mtx_wait_starttime);
    }

    // Try to find the entry in the owners
    LockEntry * en = owners[hash(txn->get_txn_id())];
    LockEntry * prev = NULL;
    while (en != NULL && en->txn != txn) {
        prev = en;
        en = en->next;
    }

    if (en) {
        if (txn->is_commit) {
            pthread_mutex_lock(txn->ts_interval_lock);
            if (en->type == LOCK_SH) {
                last_committed_read_ts = max(last_committed_read_ts, txn->get_tcm_early());
            } else {
                assert(en->type == LOCK_EX);
                last_committed_write_ts = max(last_committed_write_ts, txn->get_tcm_early());
            }
            pthread_mutex_unlock(txn->ts_interval_lock);
        }
        if (prev) 
            prev->next = en->next;
        else 
            owners[hash(txn->get_txn_id())] = en->next;
        owner_cnt --;
        if (owner_cnt == 0) {
            INC_STATS(txn->get_thd_id(),twopl_owned_cnt,1);
            uint64_t endtime = get_sys_clock();
            INC_STATS(txn->get_thd_id(),twopl_owned_time,endtime - own_starttime);
            if(lock_type == LOCK_SH) {
                INC_STATS(txn->get_thd_id(),twopl_sh_owned_time,endtime - own_starttime);
                INC_STATS(txn->get_thd_id(),twopl_sh_owned_cnt,1);
            }
            else {
                INC_STATS(txn->get_thd_id(),twopl_ex_owned_time,endtime - own_starttime);
                INC_STATS(txn->get_thd_id(),twopl_ex_owned_cnt,1);
            }
            lock_type = LOCK_NONE;
        } else {
            if (en->type == LOCK_EX) {
                assert(lock_type == LOCK_EX);
                lock_type = LOCK_SH;
            }
        }
        return_entry(en);
    } else {
        // find from wait list
        en = waiters_head;
        while (en != NULL && en->txn != txn)
            en = en->next;
        assert(en);

        LIST_REMOVE(en);
        if (en == waiters_head)
            waiters_head = en->next;
        if (en == waiters_tail)
            waiters_tail = en->prev;
        
        waiter_cnt --;
        return_entry(en);
    }

    // grant lock to wait list
    if (lock_type == LOCK_SH || lock_type == LOCK_NONE) {
        LockEntry * entry;
        while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
            LIST_GET_HEAD(waiters_head, waiters_tail, entry);
            uint64_t timespan = get_sys_clock() - entry->txn->twopl_wait_start;
            entry->txn->twopl_wait_start = 0;

            INC_STATS(txn->get_thd_id(),twopl_wait_time,timespan);\

            STACK_PUSH(owners[hash(entry->txn->get_txn_id())], entry);
            owner_cnt ++;
            waiter_cnt --;
            if(entry->txn->get_timestamp() > max_owner_ts) {
                max_owner_ts = entry->txn->get_timestamp();
            }

            ASSERT(entry->txn->lock_ready == false);
            if(entry->txn->decr_lr() == 0) {
                if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                    txn_table.restart_txn(txn->get_thd_id(),entry->txn->get_txn_id(),entry->txn->get_batch_id());
                }
            }
            if(lock_type == LOCK_NONE) {
                own_starttime = get_sys_clock();
            }
            lock_type = entry->type;
        }
        // assert(lock_type != LOCK_EX);
        if (waiters_head && waiters_head->type == LOCK_EX && lock_type == LOCK_SH) {
            LIST_GET_HEAD(waiters_head, waiters_tail, entry);
            STACK_PUSH(owners[hash(entry->txn->get_txn_id())], entry);
            owner_cnt ++;
            waiter_cnt --;
            if(entry->txn->get_timestamp() > max_owner_ts) {
                max_owner_ts = entry->txn->get_timestamp();
            }

            ASSERT(entry->txn->lock_ready == false);
            if(entry->txn->decr_lr() == 0) {
                if(ATOM_CAS(entry->txn->lock_ready,false,true)) {
                    txn_table.restart_txn(txn->get_thd_id(),entry->txn->get_txn_id(),entry->txn->get_batch_id());
                }
            }
            lock_type = entry->type;
        }
    }

    uint64_t timespan = get_sys_clock() - starttime;
    txn->txn_stats.cc_time += timespan;
    txn->txn_stats.cc_time_short += timespan;
    INC_STATS(txn->get_thd_id(),twopl_release_time,timespan);
    INC_STATS(txn->get_thd_id(),twopl_release_cnt,1);

    if (g_central_man)
        glob_manager.release_row(_row);
    else
        pthread_mutex_unlock( latch );
    // printf("lock_release end, txn_id: %lu\n", txn->get_txn_id());

    return rc;
}

void Row_tcm::push_lock_stack(TxnManager *txn, lock_t type) {
    // grant lock-sh
    LockEntry * entry = get_entry();
    entry->type = type;
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    STACK_PUSH(owners[hash(txn->get_txn_id())], entry);

    if(txn->get_timestamp() > max_owner_ts) {
        max_owner_ts = txn->get_timestamp();
    }
    owner_cnt ++;
}

void Row_tcm::lock_ts_interval(TxnManager *txn1, TxnManager *txn2) {
    assert(txn1->get_txn_id() != txn2->get_txn_id());
    if (txn1->get_txn_id() < txn2->get_txn_id()) {
        pthread_mutex_lock(txn1->ts_interval_lock);
        pthread_mutex_lock(txn2->ts_interval_lock);
    } else {
        pthread_mutex_lock(txn2->ts_interval_lock);
        pthread_mutex_lock(txn1->ts_interval_lock);
    }
}

void Row_tcm::unlock_ts_interval(TxnManager *txn1, TxnManager *txn2) {
    assert(txn1->get_txn_id() != txn2->get_txn_id());
    if (txn1->get_txn_id() < txn2->get_txn_id()) {
        pthread_mutex_unlock(txn1->ts_interval_lock);
        pthread_mutex_unlock(txn2->ts_interval_lock);
    } else {
        pthread_mutex_unlock(txn2->ts_interval_lock);
        pthread_mutex_unlock(txn1->ts_interval_lock);
    }
}

RC Row_tcm::add_reader_entry_into_waiters(TxnManager *txn) {
    RC rc = WAIT;
    LockEntry * entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    entry->type = LOCK_SH;
    LockEntry * en;
    ATOM_CAS(txn->lock_ready,1,0);
    txn->incr_lr();
    en = waiters_tail;
    /****************************/
    // move as far as possible, until reach a writer that must precede it
    while (en != NULL) {
        if (en->type == LOCK_EX) {
            lock_ts_interval(txn, en->txn);
            if (!can_be_before(txn, en->txn)) {
                unlock_ts_interval(txn, en->txn);
                break;
            } else {
                put_before(txn, en->txn, true);
                unlock_ts_interval(txn, en->txn);
            }
        }
        en = en->prev;
    }
    
    if (en) {
        lock_ts_interval(txn, en->txn);
        if (can_be_before(en->txn, txn)) {
            put_before(en->txn, txn, true);
            unlock_ts_interval(txn, en->txn);
        } else {
            unlock_ts_interval(txn, en->txn);
            return Abort;
        }
        // if (txn->get_tcm_early() > txn->get_tcm_late()) {
        //     return Abort;
        // }
        LIST_INSERT_AFTER(en, entry, waiters_tail);
    } else {
        // reader can insert into head
        pthread_mutex_lock(txn->ts_interval_lock);
        if (txn->get_tcm_early() > txn->get_tcm_late()) {
            pthread_mutex_unlock(txn->ts_interval_lock);
            return Abort;
        }
        pthread_mutex_unlock(txn->ts_interval_lock);
        LIST_PUT_HEAD(waiters_head, waiters_tail, entry);
    }
    waiter_cnt ++;
    assert(rc == WAIT);
    return rc;
}

RC Row_tcm::add_writer_entry_into_waiters(TxnManager *txn) {
    RC rc = WAIT;
    LockEntry * entry = get_entry();
    entry->start_ts = get_sys_clock();
    entry->txn = txn;
    entry->type = LOCK_EX;
    LockEntry * en;
    ATOM_CAS(txn->lock_ready,1,0);
    txn->incr_lr();
    en = waiters_head;
    /****************************/
    // check timestamp interval, follow all blocked readers and writers
    while (en != waiters_tail) {
        assert(en != nullptr);
        lock_ts_interval(txn, en->txn);
        if (!can_be_before(en->txn, txn)) {
            unlock_ts_interval(txn, en->txn);
            return Abort;
        } else {
            put_before(en->txn, txn, true);
            unlock_ts_interval(txn, en->txn);
        }
        en = en->next;
    }
    // if (txn->get_tcm_early() > txn->get_tcm_late()) {
    //     return Abort;
    // }
    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
    waiter_cnt ++;
    assert(rc == WAIT);
    return rc;
}

bool Row_tcm::can_be_before(TxnManager *former, TxnManager *later) {
    if (later->get_tcm_early() >= later->get_tcm_late())
        return false;
    if (former->get_tcm_end()) {
        if (former->get_tcm_early() >= former->get_tcm_late()) {
            return false;
        }
    } else {
        if (former->get_tcm_early() >= get_sys_clock()) {
            return false;
        }
    }
    if (later->get_tcm_end() && later->get_tcm_late() <= former->get_tcm_early() + 1)
        return false;
    else 
        return true;
}

void Row_tcm::put_before(TxnManager *former, TxnManager *later, bool later_is_reader) {
    // bool check = false;
    if (!former->get_tcm_end()) {
        former->set_tcm_end(true);
        former->set_tcm_late(get_sys_clock());
        if (former->get_tcm_early() > former->get_tcm_late()) {
            // check = true;
            printf("set late error, txn id: %lu, early: %lu, late: %lu.\n", former->get_txn_id(), former->get_tcm_early(), former->get_tcm_late());
            former->set_tcm_late(former->get_tcm_early());
        }
        // assert(former->get_tcm_early() < former->get_tcm_late());
    }
    if (former->get_tcm_early() > former->get_tcm_late()) {
        printf("BBBB former, txn id: %lu, early: %lu, late: %lu.\n", former->get_txn_id(), former->get_tcm_early(), former->get_tcm_late());
    }
    if (later->get_tcm_early() > later->get_tcm_late()) {
        // check = true;
        printf("BBBB later, txn id: %lu, early: %lu, late: %lu.\n", later->get_txn_id(), later->get_tcm_early(), later->get_tcm_late());
    }
    
    uint64_t later_early = later->get_tcm_early();
    uint64_t later_late = later->get_tcm_late();
    uint64_t former_early = former->get_tcm_early();
    uint64_t former_late = former->get_tcm_late();

    if (later_is_reader) { /* former must be a write */
        later->set_tcm_early(max(later->get_tcm_early(), former->get_tcm_early() + 1));
        former->set_tcm_late(min(former->get_tcm_late(), later->get_tcm_early()));
        former->set_tcm_end(true);
        if (former->get_tcm_early() > former->get_tcm_late()) {
            printf("LRF former's txn id: %lu. [%lu, %lu]->[%lu, %lu]; later's txn id: %lu. [%lu, %lu]->[%lu, %lu].\n", former->get_txn_id(), former_early, former_late, former->get_tcm_early(), former->get_tcm_late(),
                    later->get_txn_id(), later_early, later_late, later->get_tcm_early(), later->get_tcm_late());
        }
        if (later->get_tcm_early() > later->get_tcm_late()) {
            printf("LRL former's txn id: %lu. [%lu, %lu]->[%lu, %lu]; later's txn id: %lu. [%lu, %lu]->[%lu, %lu].\n", former->get_txn_id(), former_early, former_late, former->get_tcm_early(), former->get_tcm_late(),
                    later->get_txn_id(), later_early, later_late, later->get_tcm_early(), later->get_tcm_late());
        }
    } else { 
        /* either former is a reader and get most of time range or former is a writer that currently holds a resource */
        bool flag;
        if (later->get_tcm_end()) {
            flag = true;
            former->set_tcm_late(min(former->get_tcm_late(), later->get_tcm_late() - 1));
            later->set_tcm_early(max(later->get_tcm_early(), former->get_tcm_late()));
        } 
        if (former->get_tcm_early() > former->get_tcm_late()) {
            printf("LWF former's txn id: %lu. flag: %d, [%lu, %lu]->[%lu, %lu]; later's txn id: %lu. [%lu, %lu]->[%lu, %lu].\n", former->get_txn_id(), flag == true, former_early, former_late, former->get_tcm_early(), former->get_tcm_late(),
                    later->get_txn_id(), later_early, later_late, later->get_tcm_early(), later->get_tcm_late());
        }
        if (later->get_tcm_early() > later->get_tcm_late()) {
            printf("LWL former's txn id: %lu. flag: %d, [%lu, %lu]->[%lu, %lu]; later's txn id: %lu. [%lu, %lu]->[%lu, %lu].\n", former->get_txn_id(), flag == true, former_early, former_late, former->get_tcm_early(), former->get_tcm_late(),
                    later->get_txn_id(), later_early, later_late, later->get_tcm_early(), later->get_tcm_late());
        }
    }
    
    return;
}

RC Row_tcm::adjust_timestamps_rw(TxnManager *holder, TxnManager *requester) {
    RC rc = RCOK;
    
    return rc;
}

RC Row_tcm::adjust_timestamps_wr(TxnManager *holder, TxnManager *requester) {
    RC rc = RCOK;

    return rc;
}

RC Row_tcm::adjust_timestamps_ww(TxnManager *holder, TxnManager *requester) {
    RC rc = RCOK;

    return rc;
}

bool Row_tcm::conflict_lock(lock_t l1, lock_t l2) {
    if (l1 == LOCK_NONE || l2 == LOCK_NONE)
        return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else
        return false;
}

LockEntry * Row_tcm::get_entry() {
    LockEntry * entry = (LockEntry *)
    mem_allocator.alloc(sizeof(LockEntry));
    entry->type = LOCK_NONE;
    entry->txn = NULL;
    //DEBUG_M("row_lock::get_entry alloc %lx\n",(uint64_t)entry);
    return entry;
}

void Row_tcm::return_entry(LockEntry * entry) {
    //DEBUG_M("row_lock::return_entry free %lx\n",(uint64_t)entry);
    mem_allocator.free(entry, sizeof(LockEntry));
}
