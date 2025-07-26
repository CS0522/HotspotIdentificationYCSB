#ifndef _TWITTER_TRACE_READER_H_
#define _TWITTER_TRACE_READER_H_

#include <iostream>
#include <stdint.h>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <sys/mman.h>

namespace module
{

enum OperationType 
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
static const std::unordered_map<std::string, OperationType> g_op_map = 
{
  {"get", OperationType::GET},
  {"gets", OperationType::GETS},
  {"set", OperationType::SET},
  {"add", OperationType::ADD},
  {"replace", OperationType::REPLACE},
  {"cas", OperationType::CAS},
  {"append", OperationType::APPEND},
  {"prepend", OperationType::PREPEND},
  {"delete", OperationType::DELETE},
  {"incr", OperationType::INCR},
  {"decr", OperationType::DECR}
};

struct Request
{
  uint64_t timestamp;
  std::string anonymized_key;
  uint32_t key_size;
  uint32_t value_size;
  uint32_t client_id;
  OperationType operation;
  uint32_t ttl;
};


class TwitterTraceReader
{
private:
  std::vector<Request> trace_requests_;
  std::vector<Request>::iterator trace_iter_;
  bool read_succeeded_ = false;
  bool enable_mmap_ = false;
  void* mmap_reader_ptr_ = nullptr;

public:
  TwitterTraceReader(bool enable_mmap);
  TwitterTraceReader(bool enable_mmap, const std::string& trace_file_path);

  // @brief 内存够大情况下直接读入内存
  bool ReadTraceFile(const std::string& trace_file_path, const char delimiter = ',');
  // @brief 返回请求数组
  std::vector<Request> GetTraceRequests();

  // @brief 通过内存指针方式读取数据
  // TODO
  bool ReadTraceFileByMmap(const std::string& trace_file_path);

  Request GetFirst();
  Request GetLast();
  Request GetNext();
  Request GetPrev();

  void TraverseTrace();
};

}

#endif