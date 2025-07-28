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


TwitterTraceWorkload::TwitterTraceWorkload()
  : CoreWorkload()
{}

void TwitterTraceWorkload::Init(const utils::Properties &p) 
{ 
  YCSB_C_LOG_INFO("TwitterTraceWorkload is initializing......\n");
  
  std::string trace_file_path = p.GetProperty(TRACE_FILE_PROPERTY);
  this->twitter_trace_reader_ = new module::TwitterTraceReader(false, trace_file_path);

  // usertable
  table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);
  // fieldcount = 1
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  // get record, operation count from TwitterTraceReader
  record_count_ = this->twitter_trace_reader_->GetAllRequestsCount();
  operation_count_ = record_count_;

  // jump to first request
  this->twitter_trace_reader_->JumpToFirst();

  YCSB_C_LOG_INFO("TwitterTraceWorkload is initialized\n");
  YCSB_C_LOG_INFO("\ttable_name: %s\n", table_name_.c_str());
  YCSB_C_LOG_INFO("\record_count: %zu\n", record_count_);
}

void TwitterTraceWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values) 
{
  for (int i = 0; i < field_count_; ++i) 
  {
    ycsbc::DB::KVPair pair;
    pair.first.append("field").append(std::to_string(i));
    // 在这里根据 MaxValueSize 长度生成一个随机字符串
    size_t max_value_size = this->twitter_trace_reader_->GetCurrentValueSize();
    pair.second.append(max_value_size, utils::RandomPrintChar());
    values.push_back(pair);
  }
}

void TwitterTraceWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  // 在这里根据 ValueSize 长度生成一个随机字符串
  size_t value_size = this->twitter_trace_reader_->GetCurrentValueSize();
  pair.second.append(value_size, utils::RandomPrintChar());
  update.push_back(pair);
}

inline std::string TwitterTraceWorkload::NextSequenceKey() 
{
  // may be null string
  return this->twitter_trace_reader_->GetNextKey();
}

inline std::string TwitterTraceWorkload::NextTransactionKey() 
{
  // may be null string
  return this->twitter_trace_reader_->GetNextKey();
}

inline Operation TwitterTraceWorkload::NextOperation()
{
  module::TwitterTraceOperation twitter_trace_op = this->twitter_trace_reader_->GetNextOperationWithoutForward();
  switch (twitter_trace_op) 
  {
    case module::TwitterTraceOperation::GET:
      return Operation::READ;
    case module::TwitterTraceOperation::SET:
    case module::TwitterTraceOperation::REPLACE:
      return Operation::UPDATE;
    case module::TwitterTraceOperation::ADD:
      return Operation::INSERT;
    case module::TwitterTraceOperation::GETS:
      return Operation::SCAN;
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