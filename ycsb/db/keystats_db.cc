#include "db/keystats_db.h"
#include <fstream>

namespace ycsbc {

void KeyStatsDB::Init()
{
  std::cout << "A new thread of KeyStatsDB begins working. " << std::endl;

  // 避免 DelegateClient() 中重复初始化模块
  if (this->heat_separators.size())
    return;
  // 初始化 Heat Separator 模块
  // TODO: 调参
  this->heat_separators.emplace_back(new module::HeatSeparatorLruK(3, 200));
  this->heat_separators.emplace_back(new module::HeatSeparatorWindow(std::chrono::milliseconds(5), 3));
  this->heat_separators.emplace_back(new module::HeatSeparatorSketch(200, 0.001, 0.01, 3));

  std::cout << "Heat Separator Modules are initialized. " << std::endl;
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

  for (auto& hs : this->heat_separators)
    hs->Get(key);

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

  for (auto& hs : this->heat_separators)
    hs->Put(key);

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

  for (auto& hs : this->heat_separators)
    hs->Put(key);
  
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
  std::cout << "===========" << std::endl << "Total Key Num: " << total_count << std::endl;

  // 输出到文件
  std::fstream output_file("./key_stats.csv", std::ios::out);
  if (!output_file.is_open())
  {
    std::cerr << "key_stats.csv open failed" << std::endl;
    exit(EXIT_FAILURE);    
  }
  for (const auto& ks : ordered_key_stats)
    output_file << ks.first << "," << ks.second << std::endl;
  output_file.close();
  std::cout << "key_stats.csv is generated successfully" << std::endl;

  // 冷热识别模块输出到文件
  std::vector<std::string> hot_keys;
  for (size_t i = 0; i < this->heat_separators.size(); i++)
  {
    // join file name
    std::string file_name = "./hotkeys_" + this->heat_separators[i]->GetName() + ".csv";
    hot_keys.clear();
    this->heat_separators[i]->GetHotKeys(hot_keys);
    std::fstream hk_file(file_name, std::ios::out);
    if (!hk_file.is_open())
    {
      std::cerr << file_name << " open failed" << std::endl;
      exit(EXIT_FAILURE);
    }
    for (auto const& h_k : hot_keys)
      hk_file << h_k << std::endl;
    hk_file.close();

    std::cout << file_name << " is generated successfully" << std::endl;
  }
}

} // ycsbc

