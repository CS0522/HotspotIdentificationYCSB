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

// 目前只需要 key、key_size、value_size、operation
struct Request
{
  // uint64_t timestamp;
  std::string anonymized_key;
  // key_size = len(anonymized_key)
  uint32_t key_size;
  uint32_t value_size;
  // uint32_t client_id;
  TwitterTraceOperation operation;
  // uint32_t ttl;
};

/**
 * 读取 Twitter Cache-trace
 * 采用分片读取方式，每个线程读取自己范围（交错读取）
 * 缺点是无法保序（Trace 中顺序）
 */
class TwitterTraceReader
{
private:
  std::vector<Request> trace_requests_;

  size_t limit_record_count_ = 0;
  size_t limit_operation_count_ = 0;

  // 单线程（已弃用）
  std::vector<Request>::iterator trace_iter_;
  Request* curr_request_ptr_ = nullptr;

  // 多线程
  size_t thread_count_;
  // 多线程局部索引计数
  std::vector<std::atomic<size_t>> thread_local_index_;
  
  bool read_succeeded_ = false;

  bool CheckThreadId(const size_t thread_id);

public:
  TwitterTraceReader();
  TwitterTraceReader(const std::string& trace_file_path);
  TwitterTraceReader(const std::string& trace_file_path, const size_t thread_count);
  TwitterTraceReader(const std::string& trace_file_path, const size_t thread_count, 
                      const size_t limit_record_count, const size_t limit_operation_count);

  // @brief 内存够大情况下直接读入内存，单线程
  bool ReadTraceFile(const std::string& trace_file_path, const char delimiter = ',');
  // @brief 返回请求数组
  bool GetTraceRequests(std::vector<Request>& requests);
  // @brief 返回 Trace 中所有请求个数（操作数）
  size_t GetAllRequestsCount();

  // only single-thread
  Request* JumpToFirst();
  // only single-thread
  Request* JumpToLast();
  Request* GetNext();
  Request* GetNextByThread(size_t thread_id);
  std::string GetNextKey();
  std::string GetNextKeyByThread(size_t thread_id);
  
  Request* GetCurrent();
  Request* GetCurrentByThread(size_t thread_id);
  size_t GetCurrentValueSize();
  size_t GetCurrentValueSizeByThread(size_t thread_id);
  size_t GetCurrentKeySize();
  size_t GetCurrentKeySizeByThread(size_t thread_id);
  std::string GetCurrentKey();
  std::string GetCurrentKeyByThread(size_t thread_id);
  
  Request* GetPrev();
  Request* GetPrevByThread(size_t thread_id);
  std::string GetPrevKey();
  std::string GetPrevKeyByThread(size_t thread_id);

  TwitterTraceOperation GetOperation();
  TwitterTraceOperation GetOperationByThread(size_t thread_id);

  void ResetIterator();

  // 单线程
  void TraverseTrace();
};

void DisplayRequest(const Request& req);

};

#endif