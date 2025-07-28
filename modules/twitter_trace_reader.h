#ifndef _TWITTER_TRACE_READER_H_
#define _TWITTER_TRACE_READER_H_

#include <iostream>
#include <stdint.h>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/mman.h>
#include <atomic>

#include "core/utils.h"

namespace module
{

enum TwitterTraceOperation 
{
  GET = 0,
  GETS = 1,
  SET = 2,
  ADD = 3,
  REPLACE = 4,
  CAS = 5,
  APPEND = 6,
  PREPEND = 7,
  DELETE = 8,
  INCR = 9,
  DECR = 10
};

// 字符串 op --> 枚举类型映射
static const std::unordered_map<std::string, TwitterTraceOperation> g_op_map = 
{
  {"get", TwitterTraceOperation::GET},
  {"gets", TwitterTraceOperation::GETS},
  {"set", TwitterTraceOperation::SET},
  {"add", TwitterTraceOperation::ADD},
  {"replace", TwitterTraceOperation::REPLACE},
  {"cas", TwitterTraceOperation::CAS},
  {"append", TwitterTraceOperation::APPEND},
  {"prepend", TwitterTraceOperation::PREPEND},
  {"delete", TwitterTraceOperation::DELETE},
  {"incr", TwitterTraceOperation::INCR},
  {"decr", TwitterTraceOperation::DECR}
};

struct Request
{
  uint64_t timestamp;
  std::string anonymized_key;
  // key_size = len(anonymized_key)
  uint32_t key_size;
  uint32_t value_size;
  uint32_t client_id;
  TwitterTraceOperation operation;
  uint32_t ttl;
};


class TwitterTraceReader
{
private:
  std::vector<Request> trace_requests_;
  std::vector<Request>::iterator trace_iter_;
  Request* curr_request_ptr_ = nullptr;
  
  bool read_succeeded_ = false;
  bool enable_mmap_ = false;
  // TODO
  void* mmap_reader_ptr_ = nullptr;


public:
  TwitterTraceReader(bool enable_mmap);
  TwitterTraceReader(bool enable_mmap, const std::string& trace_file_path);

  // @brief 内存够大情况下直接读入内存
  bool ReadTraceFile(const std::string& trace_file_path, const char delimiter = ',');
  // @brief 返回请求数组
  bool GetTraceRequests(std::vector<Request>& requests);
  // @brief 返回 Trace 中所有请求个数（操作数）
  size_t GetAllRequestsCount();

  // @brief 通过内存指针方式读取数据
  // TODO
  bool ReadTraceFileByMmap(const std::string& trace_file_path);

  Request* JumpToFirst();
  Request* JumpToLast();
  Request* GetNext();
  std::string GetNextKey();
  TwitterTraceOperation GetNextOperationWithoutForward();
  Request* GetCurrent();
  size_t GetCurrentValueSize();
  size_t GetCurrentKeySize();
  std::string GetCurrentKey();
  Request* GetPrev();
  std::string GetPrevKey();
  TwitterTraceOperation GetPrevOperationWithoutBackward();

  void TraverseTrace();
};

void DisplayRequest(const Request& req);

}

#endif