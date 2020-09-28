#ifndef SI_H
#define SI_H
#include <unordered_map>
#include <unordered_set>

// #include "../storage/row.h"
// #include "../system/global.h"
#include "row.h"
#include "global.h"
#include "semaphore.h"
#ifndef TSNODE_STRUCT
#define TSNODE_STRUCT
template <typename T>
class TSNode : public T {
 public:
  using T::T;

  template <typename ...Args>
  static TSNode<T>* push_front(std::atomic<TSNode<T>*>& head, Args&&... args) {
      TSNode<T>* new_head = new TSNode<T>(std::forward<Args>(args)...);
      new_head->next_ = head.load(std::memory_order_relaxed);
      while (!head.compare_exchange_weak(new_head->next_, new_head, std::memory_order_release, std::memory_order_relaxed));
      return new_head;
  }

  TSNode* next_ = nullptr;
};
#endif
class Dli {
 public:
  using RWSet = std::map<row_t*, uint64_t>;
  struct ValidatedTxn {
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
    ValidatedTxn(const RWSet& rset, const RWSet& wset, const ts_t start_ts, const uint64_t lower, const uint64_t upper) : is_abort_(false), rset_(rset), wset_(wset), start_ts_(start_ts), lower_(lower), upper_(upper) {}
#else
    ValidatedTxn(const RWSet& rset, const RWSet& wset, const ts_t start_ts) : is_abort_(false), rset_(rset), wset_(wset), start_ts_(start_ts) {}
#endif
    ValidatedTxn(ValidatedTxn&&) = default;
    std::atomic<bool> is_abort_;
    RWSet rset_;
    RWSet wset_;
    const ts_t start_ts_;
#if CC_ALG == DLI_DTA || CC_ALG == DLI_DTA2 || CC_ALG == DLI_DTA3
    uint64_t lower_;
    uint64_t upper_;
#endif
  };

  void init();
  RC validate(TxnManager* txn, const bool lock_rows = true);
  void finish_trans(RC rc, TxnManager* txn);
  RC find_bound(TxnManager* txn);
  static void get_rw_set(TxnManager* txn, RWSet& rset,
                         RWSet& wset);
  void latch();
  void release();

  std::atomic<TSNode<ValidatedTxn>*> validated_txns_;

  // pthread_mutex_t validated_trans_mutex_;
  // pthread_mutex_t commit_trans_rset_mutex_;
  // pthread_mutex_t commit_trans_wset_mutex_;
  // pthread_mutex_t commit_trans_lowup_mutex_;
  pthread_mutex_t mtx_;
};

#endif
