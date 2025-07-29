#include "db/keystats_db.h"
#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

// portion of all keys
static double g_hot_key_portion;

namespace ycsbc {

void KeyStatsDB::Init()
{
  YCSB_C_LOG_INFO("A new thread of KeyStatsDB begins working");

  // 避免 DelegateClient() 中重复初始化模块
  if (this->has_init_.load())
    return;
  
  if (this->enable_hotspot_identification_.load())
  {
    YCSB_C_LOG_INFO("Hotspot Idendification is enabled");
    // 读取 config 文件
    std::fstream config_file("./modules/separator_config.json", std::ios::in);
    if (!config_file.is_open())
    {
      YCSB_C_LOG_ERROR("separator_config.json open failed");
      exit(EXIT_FAILURE);
    }
    json config;
    try
    {
      config_file >> config;
    }
    catch (const json::parse_error& e)
    {
      YCSB_C_LOG_ERROR("json parse error: %s", e.what());
      exit(EXIT_FAILURE);
    }
    // 创建热识别模块
    size_t operationcount = config["operationcount"].get<size_t>();
    g_hot_key_portion = config["hot_key_portion"].get<double>();
    for (auto& module_config : config["heat_separators"]) 
    {
      std::string type = module_config["type"];
      if (type == "lru") 
      {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorLru(
            params["capacity"].get<size_t>()
          )
        );
      }
      else if (type == "lfu")
      {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorLfu(
            params["capacity"].get<size_t>(),
            params["min_freq"].get<size_t>()
          )
        );
      }
      else if (type == "lruk") 
      {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorLruK(
            params["k"].get<uint32_t>(),
            params["capacity"].get<size_t>()
          )
        );
      }
      else if (type == "window") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorWindow(
            std::chrono::milliseconds(params["window_size"].get<int>()),
            params["threshold"].get<uint32_t>()
          )
        );
      }
      else if (type == "sketch_window") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorSketch(
            params["window_size"].get<size_t>(),
            params["epsilon"].get<double>(),
            params["delta"].get<double>(),
            params["threshold"].get<size_t>(),
            params["enable_lru"].get<bool>()
          )
        );
      }
      else if (type == "w_tinylfu") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSepratorWTinyLFU(
            params["capacity"].get<size_t>()
          )
        );
      }
      else if (type == "lirs") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorLIRS(
            params["capacity"].get<size_t>()
          )
        );
      }
      else if (type == "s3_fifo") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSeparatorS3FIFO(
            params["capacity"].get<size_t>()
          )
        );
      }
      else if (type == "arc") {
        auto params = module_config["params"];
        heat_separators.emplace_back(
          new module::HeatSepratorWTinyLFU(
            params["capacity"].get<size_t>()
          )
        );
      }
    }
    YCSB_C_LOG_INFO("Heat Separator Modules are initialized ");
  }

  this->has_init_.store(true);
}

void KeyStatsDB::SetHotspotEnabled(bool new_val)
{
  this->enable_hotspot_identification_.store(new_val);
}

int KeyStatsDB::Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  YCSB_C_LOG_INFO("READ: %s, key_size: %zu", key.c_str(), key.size());
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;

  if (this->enable_hotspot_identification_.load())
  {
    for (auto& hs : this->heat_separators)
      hs->Get(key);
  }

  return 0;
}

int KeyStatsDB::Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
  YCSB_C_LOG_ERROR("SCAN is not support");
  return 0;
}

int KeyStatsDB::Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  YCSB_C_LOG_INFO("UPDATE: %s, key_size: %zu, value_size: %zu", key.c_str(), key.size(), values[0].second.size());
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;

  if (this->enable_hotspot_identification_.load())
  {
    for (auto& hs : this->heat_separators)
      hs->Put(key);
  }

  return 0;
}

int KeyStatsDB::Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  YCSB_C_LOG_INFO("INSERT: %s, key_size: %zu, value_size: %zu", key.c_str(), key.size(), values[0].second.size());
#endif
  if (start_stats_.load())
    this->key_stats_[key] += 1;

  if (this->enable_hotspot_identification_.load())
  {
    for (auto& hs : this->heat_separators)
      hs->Put(key);
  }
  
  return 0;
}             

int KeyStatsDB::Delete(const std::string &table, const std::string &key)
{
  std::lock_guard<std::mutex> lock(this->key_stats_mtx_);
#ifdef DEBUG
  YCSB_C_LOG_INFO("DELETE: %s", key.c_str());
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
  std::vector<std::pair<std::string, int64_t>> key_stats_freq_descend(this->key_stats_.begin(), this->key_stats_.end());
  // 降序
  auto descend_cmp = [](const auto& a, const auto& b) {
        return a.second > b.second;
  };
  std::sort(key_stats_freq_descend.begin(), key_stats_freq_descend.end(), descend_cmp);
  std::vector<std::pair<std::string, int64_t>> key_stats_dict_ordered(this->key_stats_.begin(), this->key_stats_.end());
  // 字典排序
  auto dict_cmp = [](const auto& a, const auto& b) {
      if (a.first.size() != b.first.size()) 
        return a.first.size() < b.first.size();
      return a.first < b.first;
  };
  std::sort(key_stats_dict_ordered.begin(), key_stats_dict_ordered.end(), dict_cmp);

  for (const auto& ks : key_stats_freq_descend)
  {
    total_count++;
    if (ks.second <= 1)
      continue;
  }
  YCSB_C_LOG_INFO("===========\nTotal Key Num: %zu", total_count);

  // 输出到文件
  std::fstream output_file_original("./key_stats.csv", std::ios::out);
  std::fstream output_file_descend("./key_stats_descend.csv", std::ios::out);
  std::fstream output_file_dict_ordered("./key_stats_dict_ordered.csv", std::ios::out);
  std::fstream output_file_hotkeys("./key_stats_hotkeys.csv", std::ios::out);
  if (!output_file_original.is_open() || !output_file_descend.is_open() || !output_file_dict_ordered.is_open())
  {
    YCSB_C_LOG_ERROR("key_stats csv file open failed");
    exit(EXIT_FAILURE);    
  }
  for (const auto& ks : this->key_stats_)
    output_file_original << ks.first << "," << ks.second << std::endl;
  output_file_original.close();
  YCSB_C_LOG_INFO("key_stats.csv is generated successfully");
  for (const auto& ks : key_stats_freq_descend)
    output_file_descend << ks.first << "," << ks.second << std::endl;
  output_file_descend.close();
  YCSB_C_LOG_INFO("key_stats_descend.csv is generated successfully");
  for (const auto& ks : key_stats_dict_ordered)
    output_file_dict_ordered << ks.first << "," << ks.second << std::endl;
  output_file_dict_ordered.close();
  YCSB_C_LOG_INFO("key_stats_dict_ordered.csv is generated successfully");
  // Top-N 的键视为热 key
  YCSB_C_LOG_INFO("Hot Key Portion: %lf", g_hot_key_portion);
  size_t portion_threshold = static_cast<size_t>(key_stats_freq_descend.size() * g_hot_key_portion);
  for (size_t i = 0; i < key_stats_freq_descend.size(); i++)
  {
    if (i <= portion_threshold)
      output_file_hotkeys << key_stats_freq_descend[i].first << std::endl;
  }
  output_file_hotkeys.close();
  YCSB_C_LOG_INFO("key_stats_hotkeys.csv is generated successfully");

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
      YCSB_C_LOG_ERROR("%s open failed", file_name.c_str());
      exit(EXIT_FAILURE);
    }
    for (auto const& h_k : hot_keys)
      hk_file << h_k << std::endl;
    hk_file.close();

    YCSB_C_LOG_INFO("%s is generated successfully", file_name.c_str());
  }
}

} // ycsbc

