#ifndef _KEYSTATS_DB_H_
#define _KEYSTATS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

#include <atomic>
#include <unordered_map>

namespace ycsbc {

class KeyStatsDB : public DB {
public:
  void Init();
  
  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Delete(const std::string &table, const std::string &key);

  int Special(const std::string &command);

private:
  std::mutex key_stats_mtx_;
  // 引入 哈希表 存储 Key 对应的统计计数
  std::unordered_map<std::string, int64_t> key_stats_;
  // 开始统计的标志位
  std::atomic<bool> start_stats_{false};
};

} // ycsbc

#endif // _KEYSTATS_DB_H_

