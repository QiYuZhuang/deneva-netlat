#ifndef _ADAPTOR_H_
#define _ADAPTOR_H_

#include "storage/row.h"
#include "global.h"
#include <string>
#include <random>
#include <time.h>

#define MAX_ABORT (0.05)
#define STATE_COUNT 3
#define LEVEL_COUNT 3
#define IFFAIL(_condition) ((_condition) != RC::RCOK)

enum AdaptorStat{INVAILD = -1, WARM, CAL, END} ;

class adaptor
{
public:
  adaptor();

public:
  AdaptorStat get_state();
  int64_t get_interval(row_t *cur_w);
  RC run();
  void inc_commit_count() { commit_count_++; }
  void inc_abort_count() { abort_count_++; }
  double get_abort_ratio() { return abort_count_ * 1.0 / (abort_count_ + commit_count_ + 1); };
  
private:
  RC adaptor_warm();
  RC adaptor_cal();
  // void set_conflict_level();
  // void update_conflict_num();
  void enable_print() { en_print_ = true; }

private:
  AdaptorStat state_;
  // const int64_t poll_interval_;
  int32_t time_interval_[LEVEL_COUNT]; // the value set when write after read;    
  int64_t commit_count_;
  int64_t abort_count_;
  int64_t max_iter_;

// SA
public:
  void init_SA_Params(double t_initial, double t_denominator, double k, double t_min, int32_t epochs_fixed_T, int64_t seed, int32_t max_interval);
  void solve(int32_t level);
private:
  double take_step(int32_t level);
  void undo_step();
  void save_best(int32_t level);
  double energy();
  void print();
  double boltzmann(double E, double new_E, double T, double k);
private:
  bool en_print_;
  int64_t seed_;
  double k_;
  double t_initial_;
  double t_denominator_;
  double t_min_;
  int32_t epochs_fixed_T_;
  std::mt19937 rand_generator_;
  int32_t best_interval_;
  int32_t max_interval_; // let random() generate a vaild interval
};

#endif
