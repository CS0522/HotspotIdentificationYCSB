#ifndef _TWITTER_TRACE_WORKLOAD_H_
#define _TWITTER_TRACE_WORKLOAD_H_

#include "core_workload.h"
#include "modules/twitter_trace_reader.h"

/**
 * 逻辑：
 * 1. 包含 TwitterTraceReader 读取 Trace 文件；
 * 2. Load 阶段：根据 Trace 中所有 Key 预先插入数据。按照 RubbleDB 的逻辑，将 Trace 中每一个请求（不管get\set）都插入；
 * 3. Run 阶段：按照 Trace 下负载，不考虑 Timestamp。
 */

namespace ycsbc {

class TwitterTraceWorkload : public CoreWorkload
{
 public:
  /// The name of the database table to run queries against.
  static const std::string TABLENAME_PROPERTY;
  static const std::string TABLENAME_DEFAULT;
  
  /// The name of the property for the number of fields in a record.
  static const std::string FIELD_COUNT_PROPERTY;
  static const std::string FIELD_COUNT_DEFAULT;

  // Add trace file property
  static const std::string TRACE_FILE_PROPERTY;
  
  virtual void Init(const utils::Properties &p);
  
  virtual void BuildValues(std::vector<ycsbc::DB::KVPair> &values, const std::string& key);
  virtual void BuildUpdate(std::vector<ycsbc::DB::KVPair> &update);
  
  virtual std::string NextTable() { return table_name_; }
  /// Used for loading data
  virtual std::string NextSequenceKey();
  /// Used for transactions
  virtual std::string NextTransactionKey();
  virtual Operation NextOperation();
  virtual std::string NextFieldName();

  virtual size_t GetRecordCount();
  virtual size_t GetOperationCount();

  TwitterTraceWorkload();
  ~TwitterTraceWorkload() {}
  
 protected:
  std::string BuildKeyName(uint64_t key_num);

  std::string table_name_;
  int field_count_;
  size_t record_count_;
  size_t operation_count_;

  module::TwitterTraceReader *twitter_trace_reader_ = nullptr;
};
  
}

#endif
