#include "adaptor.h"
#define GSL_LOG_DBL_MIN   (-7.0839641853226408e+02)

adaptor::adaptor() {
    state_ = WARM;
    max_iter_ = 100;
    commit_count_ = 0;
    abort_count_ = 0;
    // memset(time_interval, 0, LEVEL_COUNT * sizeof(int64_t));
    for (int32_t i = 0; i < LEVEL_COUNT; i++) {
        time_interval_[i] = 1;
    }
}

AdaptorStat adaptor::get_state() 
{
	return state_;
}

int64_t adaptor::get_interval(row_t *cur_w) 
{
	return time_interval_[cur_w->conflict_level];
}

RC adaptor::adaptor_warm() 
{
    // collect information in 10 seconds
    RC rc = RCOK;
    usleep(100); // sleep 1 ms
    state_ = CAL;
    return rc;
}

// confirm the intervals
RC adaptor::adaptor_cal() 
{
    RC rc = RCOK;
	// set_hot_level();
	// assign different time interval to different hot levels
    // update_conflict_num();
    // enable_print();
    for (int i = 1; i < LEVEL_COUNT; i++) {
      // heuristic search to different condlict level
      // 1. SA
        init_SA_Params(1, 1.5, 1, 0.01, 2 /* 2 epochs in fix T */, (unsigned)time(NULL) /* seed */, 5 * i /* max interval */);
        solve(i);
        time_interval_[i] = best_interval_;
        best_interval_ = 1;
    }
    // if (en_print_) {
    //     printf("|******** CALCULATE FINISH ********|\n");
    //     printf("| low conflict level: %12d |\n", time_interval_[0]);
    //     printf("| normal conflict level: %9d |\n", time_interval_[1]);
    //     printf("| high conflict level: %11d |\n", time_interval_[2]);
    //     printf("|----------------------------------|\n");
    //     fflush(stdout);
    // }
	  state_ = END;
    return rc;
}

RC adaptor::run() 
{
	  RC rc = RCOK;
    // sleep(10);
    adaptor_warm();
// Calculating:
    adaptor_cal();

// apply best interval
    // printf("[INFO] (zhuang).4 run here.\n");
    // fflush(stdout);
    abort_count_ = 0;
    commit_count_ = 0;
    uint64_t start_time = get_sys_clock(); //  1000 ns = 1 us
    uint64_t end_time = start_time;
    // printf("[INFO] (zhuang).2 time now. start_time: %lu.\n", start_time);
    // fflush(stdout);
    // while (1) {
    // printf("[INFO] (zhuang).9 here.\n");
    // fflush(stdout);
    while(!simulation->is_done()) {
        end_time = get_sys_clock();
        // printf("[INFO] (zhuang).3 . commit_count: %ld, abort_count: %ld\n", commit_count_, abort_count_);
        // printf("[INFO] (zhuang).8 . start_time: %lu, end_time: %lu\n", start_time, end_time);
        // fflush(stdout);
        // double a_ratio = energy();
        if (energy() > MAX_ABORT) {
            // TODO (zhuang): modify the time of thread sleep
            // printf("[INFO] (zhuang).1 sleep time. sleep time: %u, abort ratio: %f\n", g_adaptor_sleep_time, a_ratio);
            // fflush(stdout);
            g_adaptor_sleep_time = 100;
            state_ = CAL;
            break;
        } else if ((end_time - start_time) >= 4 * g_adaptor_sleep_time) {
            if (g_adaptor_sleep_time < 5 * MILLION) {
              g_adaptor_sleep_time <<= 1;
            }
            state_ = CAL;
            break;
        } else {
          usleep(g_adaptor_sleep_time); // sleep 1 ms
        }
    } 
	  return rc;
}

void adaptor::init_SA_Params(double t_initial, double t_denominator, double k, double t_min, int32_t epochs_fixed_T, int64_t seed, int32_t max_interval) 
{
  t_initial_ = t_initial;
  t_denominator_ = t_denominator;
  k_ = k;
  t_min_ = t_min;
  epochs_fixed_T_ = epochs_fixed_T;
//   rand_generator_(seed);
  seed_ = seed;
  max_interval_ = max_interval;
  best_interval_ = 0;
}

void adaptor::solve(int32_t level)
{
  double E = energy(); // return abort ratio
  double best_E = E;
  save_best(level); // 将当前状态存为最佳解
  double T = t_initial_;
  double T_factor = 1.0 / t_denominator_;
  std::uniform_real_distribution<> dis(0, 1);
  // while (1) {
  while(!simulation->is_done()) {
    for (int32_t i = 0; i < epochs_fixed_T_; i++) {
      double new_E = take_step(level);
      if(new_E <= best_E) {
        best_E = new_E;
        save_best(level);
      }
      if (new_E < E) {
        if (new_E < best_E) {
            best_E = new_E;
            save_best(level);
        }
       E = new_E;
      } else if (dis(rand_generator_) < boltzmann(E, new_E, T, k_)) {
       E = new_E;
      } else {
        undo_step();
      }
    }
    /* apply the cooling schedule to the temperature */
   T *= T_factor;

    if (T < t_min_) {
      break;
    }
  }
}
inline double adaptor::boltzmann(double E, double new_E, double T, double k)
{
  double x = -(new_E - E) / (k * T);
 // avoid underflow errors for large uphill steps
  return (x < GSL_LOG_DBL_MIN) ? 0.0 : exp(x);
}
inline double adaptor::energy() 
{
 return abort_count_ * 1.0 / (abort_count_ + commit_count_ + 0.1);
}

inline void adaptor::undo_step()
{

}

inline double adaptor::take_step(int32_t level)
{
    // generate new interval
    abort_count_ = 0;
    commit_count_ = 0;
    std::uniform_int_distribution<> dis(1, max_interval_);
    time_interval_[level] = dis(rand_generator_);

    // collect data
    usleep(100);
    // sleep(5);
    return energy();
}

void adaptor::save_best(int32_t level)
{
  best_interval_ = time_interval_[level];
}