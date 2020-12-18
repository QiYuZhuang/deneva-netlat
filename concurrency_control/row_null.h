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

#ifndef ROW_NULL_H
#define ROW_NULL_H
#include "../storage/row.h"
class table_t;
class Catalog;
class TxnManager;

class Row_null {
 public:
  void init(row_t* row);
  RC access(access_t type, TxnManager* txn);
  RC abort(access_t type, TxnManager* txn);
  RC commit(access_t type, TxnManager* txn);

 private:
  row_t* _row;

   // multi-verison part
 private:
  pthread_mutex_t* latch;
  bool blatch;
};

#endif
