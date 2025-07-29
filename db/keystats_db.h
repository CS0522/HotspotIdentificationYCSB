#ifndef _KEYSTATS_DB_H_
#define _KEYSTATS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

#include <atomic>
#include <unordered_map>
#include <algorithm>
#include <vector>

#include "core/utils.h"
#include "modules/separator.h"
#include "modules/heat_separator_lru_k.h"
#include "modules/heat_separator_sketch_window.h"
#include "modules/heat_separator_window.h"
#include "modules/heat_separator_lru.h"
#include "modules/heat_separator_lfu.h"
#include "modules/heat_separator_w_tinylfu.h"
#include "modules/heat_separator_lirs.h"
#include "modules/heat_separator_s3_fifo.h"
#include "modules/heat_separator_arc.h"

namespace ycsbc {

class KeyStatsDB : public DB {
public:
  void Init();

  void SetHotspotEnabled(bool new_val);
  
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

  void OutputStats();

private:
  std::mutex key_stats_mtx_;
  // 引入 哈希表 存储 Key 对应的统计计数
  std::unordered_map<std::string, int64_t> key_stats_;
  // 已经初始化的标志位
  std::atomic<bool> has_init_{false};
  // 开始统计的标志位
  std::atomic<bool> start_stats_{false};
  // 开启热识别算法（仅单线程）
  std::atomic<bool> enable_hotspot_identification_{false};

  // 热识别算法模块
  std::vector<module::HeatSeparator*> heat_separators;
};

} // ycsbc

#endif // _KEYSTATS_DB_H_

