//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <string>
#include "db.h"
#include "core_workload.h"
#include "twitter_trace_workload.h"
#include "utils.h"

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload *wl) : db_(db), workload_(wl) { }
  
  virtual bool DoInsert(const size_t thread_id);
  virtual bool DoTransaction(const size_t thread_id);
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead(size_t thread_id);
  virtual int TransactionReadModifyWrite(size_t thread_id);
  virtual int TransactionScan(size_t thread_id);
  virtual int TransactionUpdate(size_t thread_id);
  virtual int TransactionInsert(size_t thread_id);
  
  DB &db_;
  CoreWorkload *workload_;
};

inline bool Client::DoInsert(const size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  std::string key = t_wl->NextSequenceKey(thread_id);
  std::vector<DB::KVPair> pairs;
  t_wl->BuildValues(pairs, thread_id);
  return (db_.Insert(workload_->NextTable(), key, pairs) == DB::kOK);
#else
  std::string key = workload_->NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_->BuildValues(pairs);
  return (db_.Insert(workload_->NextTable(), key, pairs) == DB::kOK);
#endif
}

inline bool Client::DoTransaction(const size_t thread_id) {
  int status = -1;
  ycsbc::Operation op;
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  op = t_wl->NextOperation(thread_id);
#else
  op = workload_->NextOperation();
#endif
  switch (op) {
    case READ:
      status = TransactionRead(thread_id);
      break;
    case UPDATE:
      status = TransactionUpdate(thread_id);
      break;
    case INSERT:
      status = TransactionInsert(thread_id);
      break;
    case SCAN:
      status = TransactionScan(thread_id);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite(thread_id);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead(size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  const std::string &table = t_wl->NextTable();
  const std::string &key = t_wl->NextTransactionKey(thread_id);
  std::vector<DB::KVPair> result;
  if (!t_wl->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + t_wl->NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
#else
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_->NextFieldName());
    return db_.Read(table, key, &fields, result);
  } else {
    return db_.Read(table, key, NULL, result);
  }
#endif
}

inline int Client::TransactionReadModifyWrite(size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  const std::string &table = t_wl->NextTable();
  const std::string &key = t_wl->NextTransactionKey(thread_id);
  std::vector<DB::KVPair> result;

  if (!t_wl->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + t_wl->NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (t_wl->write_all_fields()) {
    t_wl->BuildValues(values, thread_id);
  } else {
    t_wl->BuildUpdate(values, thread_id);
  }
  return db_.Update(table, key, values);
#else
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_->NextFieldName());
    db_.Read(table, key, &fields, result);
  } else {
    db_.Read(table, key, NULL, result);
  }

  std::vector<DB::KVPair> values;
  if (workload_->write_all_fields()) {
    workload_->BuildValues(values);
  } else {
    workload_->BuildUpdate(values);
  }
  return db_.Update(table, key, values);
#endif
}

inline int Client::TransactionScan(size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  const std::string &table = t_wl->NextTable();
  const std::string &key = t_wl->NextTransactionKey(thread_id);
  int len = t_wl->NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!t_wl->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + t_wl->NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
#else
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  int len = workload_->NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_->read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_->NextFieldName());
    return db_.Scan(table, key, len, &fields, result);
  } else {
    return db_.Scan(table, key, len, NULL, result);
  }
#endif
}

inline int Client::TransactionUpdate(size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  const std::string &table = t_wl->NextTable();
  const std::string &key = t_wl->NextTransactionKey(thread_id);
  std::vector<DB::KVPair> values;
  if (t_wl->write_all_fields()) {
    t_wl->BuildValues(values, thread_id);
  } else {
    t_wl->BuildUpdate(values, thread_id);
  }
  return db_.Update(table, key, values);
#else 
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_->write_all_fields()) {
    workload_->BuildValues(values);
  } else {
    workload_->BuildUpdate(values);
  }
  return db_.Update(table, key, values);
#endif
}

inline int Client::TransactionInsert(size_t thread_id) {
#ifdef TWITTER_TRACE
  TwitterTraceWorkload* t_wl = static_cast<TwitterTraceWorkload*>(workload_);
  const std::string &table = t_wl->NextTable();
  const std::string &key = t_wl->NextSequenceKey(thread_id);
  std::vector<DB::KVPair> values;
  t_wl->BuildValues(values, thread_id);
  return db_.Insert(table, key, values);
#else
  const std::string &table = workload_->NextTable();
  const std::string &key = workload_->NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_->BuildValues(values);
  return db_.Insert(table, key, values);
#endif
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
