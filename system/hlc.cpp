#include "global.h"
#include "manager.h"
#include "http.h"
#include "libtcpforcpp.h"
#include <stdlib.h>
#include <unistd.h>
#include <curl/curl.h>
#include <fcntl.h>
#include <unistd.h>
#include "mem_alloc.h"
#include "helper.h"

#include "hlc.h"

inline static int64_t CurrentSeconds() {
	int64_t ts = 0;
	struct timeval tv;
	if (gettimeofday(&tv, NULL)) {
		fprintf(stderr, "TDSQL: get time of day failed");
	} else {
		ts = (uint64_t)(tv.tv_sec);
	}
	return ts;
}
inline static uint64_t uint64_max(uint64_t a, uint64_t b, uint64_t c) {
	uint64_t m;
	m = a > b ? a : b;
	m = m > c ? m : c;
	return m;
}

void HLCTime::initNULLHLC(HLCTime *hlc) 
{
	hlc->m_wall_time_ = CurrentSeconds() << physicalShift;
	hlc->m_logic_time_ = 0;
}

void HLCTime::initHLC(HLCTime *hlc, uint64_t wall_time, uint64_t logic_time)
{
	hlc->m_wall_time_ = wall_time;
	hlc->m_logic_time_ = logic_time;
}

void HLCTime::hlcsend(HLCTime *hlc) {
	pthread_mutex_lock(hlc->mutex_);
	// lock(&hlc->mutex_);
	uint64_t pt = CurrentSeconds() << physicalShift;
	if (hlc->m_wall_time_ < pt) {
		hlc->m_wall_time_ = pt;
		hlc->m_logic_time_ = 0;
	} else {
		hlc->m_logic_time_ ++;
	}
	pthread_mutex_unlock(hlc->mutex_);
 	// SpinLockRelease(&hlc->mutex_);
}

void HLCTime::hlcupdate(HLCTime *chlc, HLCTime hlc) {
	pthread_mutex_lock(chlc->mutex_);
	// SpinLockAcquire(&chlc->mutex_);
	uint64_t pt = CurrentSeconds() << physicalShift;
	uint64_t old_wall_time = chlc->m_wall_time_;
	chlc->m_wall_time_ = uint64_max(chlc->m_wall_time_, pt, hlc.m_wall_time_);
	if (chlc->m_wall_time_ == hlc.m_wall_time_ && chlc->m_wall_time_ == old_wall_time) {
		chlc->m_logic_time_ = chlc->m_logic_time_ > hlc.m_logic_time_ ? 
						chlc->m_logic_time_ + 1 : hlc.m_logic_time_ + 1;
	} else if (chlc->m_wall_time_ == old_wall_time) {
		chlc->m_logic_time_ ++;
	} else if (chlc->m_wall_time_ == hlc.m_wall_time_) {
		chlc->m_logic_time_ = hlc.m_logic_time_ + 1;
	} else if (chlc->m_wall_time_ == pt) {
		chlc->m_logic_time_ = 0;
	}
	pthread_mutex_unlock(chlc->mutex_);
	// SpinLockRelease(&chlc->mutex_);
}

void HLCTime::update_with_cts(uint64_t hlc) {
	uint64_t wall_time = hlc >> physicalShift << physicalShift;
	uint64_t logic_time = hlc << (64 - physicalShift) >> (64 - physicalShift);
	HLCTime newhlc;
	newhlc.m_wall_time_ = wall_time;
	newhlc.m_logic_time_ = logic_time;
	hlcupdate(this, newhlc);
}

uint64_t HLCTime::getCurrentHLC() {
	hlcsend(this);
	uint64_t hlc = this->m_wall_time_ | this->m_logic_time_;
	return hlc;
}

void HLCTime::init() {
  mutex_ = (pthread_mutex_t*)mem_allocator.alloc(sizeof(pthread_mutex_t));
  pthread_mutex_init(mutex_, NULL);
}