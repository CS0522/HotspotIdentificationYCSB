#include "core_workload.h"
#include "twitter_trace_workload.h"

#include <string>
#include <iostream>

using ycsbc::TwitterTraceWorkload;
using std::string;

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
  record_count_ = this->twitter_trace_reader_->GetAllKeysCount();
  operation_count_ = this->twitter_trace_reader_->GetAllRequestsCount();

  YCSB_C_LOG_INFO("TwitterTraceWorkload is initialized\n");
  YCSB_C_LOG_INFO("\ttable_name: %s\n", table_name_.c_str());
  YCSB_C_LOG_INFO("\record_count_: %zu\n", record_count_);
}

void TwitterTraceWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values, const std::string& key) 
{
  for (int i = 0; i < field_count_; ++i) 
  {
    ycsbc::DB::KVPair pair;
    pair.first.append("field").append(std::to_string(i));
    // 在这里根据 MaxValueSize 长度生成一个随机字符串
    size_t max_value_size = this->twitter_trace_reader_->GetMaxValueSizeOfKey(key);
    pair.second.append(max_value_size, utils::RandomPrintChar());
    values.push_back(pair);
  }
}

void TwitterTraceWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
  update.push_back(pair);
}

inline std::string TwitterTraceWorkload::NextSequenceKey() 
{
  // may be null string
  return this->twitter_trace_reader_->GetNextSequenceKey();
}

inline std::string TwitterTraceWorkload::NextTransactionKey() {
  uint64_t key_num;
  do {
    key_num = key_chooser_->Next();
  } while (key_num > insert_key_sequence_.Last());
  return BuildKeyName(key_num);
}

inline std::string TwitterTraceWorkload::BuildKeyName(uint64_t key_num) {
  if (!ordered_inserts_) {
    key_num = utils::Hash(key_num);
  }
  std::string key_num_str = std::to_string(key_num);
  int zeros = zero_padding_ - key_num_str.length();
  zeros = std::max(0, zeros);
  return std::string("user").append(zeros, '0').append(key_num_str);
}

inline std::string TwitterTraceWorkload::NextFieldName() {
  return std::string("field").append(std::to_string(field_chooser_->Next()));
}

inline size_t TwitterTraceWorkload::GetRecordCount()
{
  return this->record_count_;
}

inline size_t TwitterTraceWorkload::GetOperationCount()
{
  return this->operation_count_;
}