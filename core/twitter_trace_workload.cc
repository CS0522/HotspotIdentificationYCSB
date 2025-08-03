#include "core_workload.h"
#include "twitter_trace_workload.h"

#include <string>
#include <iostream>

using ycsbc::TwitterTraceWorkload;
using std::string;
using namespace ycsbc;

const string TwitterTraceWorkload::TABLENAME_PROPERTY = "table";
const string TwitterTraceWorkload::TABLENAME_DEFAULT = "usertable";

const string TwitterTraceWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
// Changed default fieldcount from 10 to 1
const string TwitterTraceWorkload::FIELD_COUNT_DEFAULT = "1";

const string TwitterTraceWorkload::TRACE_FILE_PROPERTY = "tracefile";

const string TwitterTraceWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string TwitterTraceWorkload::OPERATION_COUNT_PROPERTY = "operationcount";


TwitterTraceWorkload::TwitterTraceWorkload()
  : CoreWorkload()
{}

TwitterTraceWorkload::TwitterTraceWorkload(const size_t thread_count)
  : CoreWorkload(), thread_count_(thread_count)
{}

void TwitterTraceWorkload::Init(const utils::Properties &p) 
{ 
  YCSB_C_LOG_INFO("TwitterTraceWorkload is initializing......");
  
  std::string trace_file_path = p.GetProperty(TRACE_FILE_PROPERTY);
  size_t thread_count = std::stoul(p.GetProperty("threadcount", "1"));

  // usertable
  table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);
  // fieldcount = 1
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  // get record, operation count from TwitterTraceReader
  this->record_count_ = std::stoul(p.GetProperty(RECORD_COUNT_PROPERTY));
  this->operation_count_ = std::stoul(p.GetProperty(OPERATION_COUNT_PROPERTY));

  this->twitter_trace_reader_ = new module::TwitterTraceReader(trace_file_path, thread_count, record_count_, operation_count_);
  // jump to first request
  this->twitter_trace_reader_->ResetIterator();

  YCSB_C_LOG_INFO("TwitterTraceWorkload is initialized");
}

void TwitterTraceWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values, size_t thread_id) 
{
  for (int i = 0; i < field_count_; ++i) 
  {
    ycsbc::DB::KVPair pair;
    pair.first.append("field").append(std::to_string(i));
    // 在这里根据 MaxValueSize 长度生成一个随机字符串
    size_t max_value_size = this->twitter_trace_reader_->GetCurrentValueSizeByThread(thread_id);
    pair.second.append(max_value_size, utils::RandomPrintChar());
    values.push_back(pair);
  }
}

void TwitterTraceWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update, size_t thread_id) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  // 在这里根据 ValueSize 长度生成一个随机字符串
  size_t value_size = this->twitter_trace_reader_->GetCurrentValueSizeByThread(thread_id);
  pair.second.append(value_size, utils::RandomPrintChar());
  update.push_back(pair);
}

inline std::string TwitterTraceWorkload::NextSequenceKey(size_t thread_id) 
{
  // may be null string
  return this->twitter_trace_reader_->GetNextKeyByThread(thread_id);
}

inline std::string TwitterTraceWorkload::NextTransactionKey(size_t thread_id) 
{
  // may be null string
  return this->twitter_trace_reader_->GetNextKeyByThread(thread_id);
}

inline Operation TwitterTraceWorkload::NextOperation(size_t thread_id)
{
  module::TwitterTraceOperation twitter_trace_op = this->twitter_trace_reader_->GetOperationByThread(thread_id);
  switch (twitter_trace_op) 
  {
    case module::TwitterTraceOperation::GET:
    case module::TwitterTraceOperation::GETS:
      return Operation::READ;
    case module::TwitterTraceOperation::SET:
    case module::TwitterTraceOperation::REPLACE:
      return Operation::UPDATE;
    case module::TwitterTraceOperation::ADD:
      return Operation::INSERT;
    // 这里暂时不支持 scan
    // case module::TwitterTraceOperation::GETS:
    //   return Operation::SCAN;
    case module::TwitterTraceOperation::INCR:
    case module::TwitterTraceOperation::DECR:
      return Operation::READMODIFYWRITE;
    case module::TwitterTraceOperation::DELETE:
      return Operation::UPDATE;
    default:
      return Operation::READ;
  }
}

inline std::string TwitterTraceWorkload::NextFieldName() 
{
  return std::string("field").append(std::to_string(0));
}

inline size_t TwitterTraceWorkload::GetRecordCount()
{
  return this->record_count_;
}

inline size_t TwitterTraceWorkload::GetOperationCount()
{
  return this->operation_count_;
}

inline void TwitterTraceWorkload::ResetIterator()
{
  this->twitter_trace_reader_->ResetIterator();
}