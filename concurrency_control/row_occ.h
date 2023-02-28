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

#ifndef ROW_OCC_H
#define ROW_OCC_H
#include "semaphore.h"

class table_t;
class Catalog;
class TxnManager;
struct TsReqEntry;

class Row_occ {
public:
    void init(row_t* row);
    RC access(TxnManager* txn, TsType type);
    void latch();
    // ts is the start_ts of the validating txn
    bool validate(uint64_t ts);
    void write(row_t* data, uint64_t ts);
    void release();

private:
    pthread_mutex_t* _latch;
    sem_t _semaphore;
    bool blatch;

    row_t* _row;
    // the last update time
    ts_t wts;
};

#endif
