#include "db/keystats_db.h"

namespace ycsbc {

void KeyStatsDB::Init()
{
  std::cout << "A new thread of KeyStatsDB begins working. " << std::endl;
}

int KeyStatsDB::Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  std::cout << "READ: " << key << std::endl;
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;
  return 0;
}

int KeyStatsDB::Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
  std::cout << "SCAN is not support. " << std::endl;
  return 0;
}

int KeyStatsDB::Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  std::cout << "UPDATE: " << key << std::endl;
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;
  return 0;
}

int KeyStatsDB::Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  std::cout << "INSERT: " << key << std::endl;
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;
  return 0;
}             

int KeyStatsDB::Delete(const std::string &table, const std::string &key)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  std::cout << "DELETE: " << key << std::endl;
#endif
  if (start_stats_.load())
    this->key_stats_.erase(key);
  return 0;
}

int KeyStatsDB::Special(const std::string &command)
{
  if ("PAUSE" == command)
    this->start_stats_.store(true);
  return 0;
}

void KeyStatsDB::OutputStats()
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
  
  size_t total_count = 0;
  // 降序输出的 vector
  std::vector<std::pair<std::string, int64_t>> ordered_key_stats(this->key_stats_.begin(), this->key_stats_.end());
  // 降序
  auto descend_cmp = [](const auto& a, const auto& b) {
        return a.second > b.second;
  };
  std::sort(ordered_key_stats.begin(), ordered_key_stats.end(), descend_cmp);

  for (const auto& ks : ordered_key_stats)
  {
    total_count++;
    if (ks.second <= 1)
      continue;
    std::cout << "Key: " << ks.first << ", count: " << ks.second << std::endl;
  }
  std::cout << "===========" << std::endl << "Total Count: " << total_count << std::endl;
}

} // ycsbc

