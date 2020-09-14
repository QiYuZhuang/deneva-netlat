#ifndef HTTP_TS_H
#define HTTP_TS_H

#include <stdlib.h>
#include <unistd.h>

#include <curl/curl.h>

#include <cinttypes>
#include "libtcpforcpp.h"

uint64_t CurlGetTimeStamp(void);
uint64_t TcpGetTimeStamp(void);

void ConnectToLts(void);
void CloseToLts(void);

class TcpTimestamp {
public:
  void init(int all_thd_num);
  void ConnectToLts(uint64_t thd_id); 
  void CloseToLts(uint64_t thd_id); 
  uint64_t TcpGetTimeStamp(uint64_t thd_id); 
private:
  TcpLtsSocket * socket;
  int socket_num;
  // pthread_mutex_t ** lock_;
};

#endif
