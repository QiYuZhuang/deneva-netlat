#ifndef HLC_H
#define HLC_H

/* C standard header files */
#include <stdlib.h>
#include <unistd.h>

#include <curl/curl.h>

#include <cinttypes>
#include "spin_lock.h"
#include "libtcpforcpp.h"

#define physicalShift 24

class HLCTime
{
private:
	uint64_t m_wall_time_;
	uint64_t m_logic_time_;
	pthread_mutex_t* mutex_;
	void initNULLHLC(HLCTime *hlc);
	void initHLC(HLCTime *hlc, uint64_t wall_time, uint64_t logic_time);
	void hlcsend(HLCTime *hlc);
	void hlcupdate(HLCTime *chlc, HLCTime hlc);
public:
	uint64_t getCurrentHLC();
	void update_with_cts(uint64_t hlc);
	void init();
};
#endif
