#include "heat_separator_lfu.h"
#include <iomanip>

namespace module
{

HeatSeparatorLfu::HeatSeparatorLfu(const size_t capacity, const size_t min_freq)
  : capacity(capacity), min_freq(min_freq)
{
  std::cout << "Lfu Heat Separator is initialized, min_freq = " << min_freq << ", capacity = " << capacity << std::endl;

  this->algorithm_name = "lfu";
}

Status HeatSeparatorLfu::Put(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  // 存在，更新频率
  if (this->key_freq_iter_map.find(key) != this->key_freq_iter_map.end())
  {
    auto& item = this->key_freq_iter_map[key];
    this->freq_key_map[item.first].erase(item.second);
    if (this->freq_key_map[item.first].empty() 
        && 
        item.first == this->min_freq)
      this->min_freq++;
    item.first++;
    this->freq_key_map[item.first].push_front(key);
    item.second = this->freq_key_map[item.first].begin();
    this->key_freq_iter_map[key] = item;

    return SUCCESS;
  }

  // 缓存满时淘汰 min_freq 链表的尾部
  if (this->key_freq_iter_map.size() >= capacity) 
  {
    auto old_key = this->freq_key_map[this->min_freq].back();
    this->key_freq_iter_map.erase(old_key);
    this->freq_key_map[this->min_freq].pop_back();
  }
  // 插入新数据
  this->min_freq = 1;
  this->freq_key_map[this->min_freq].push_front(key);
  this->key_freq_iter_map[key] = std::pair<size_t, std::list<std::string>::iterator>(
                                  min_freq, this->freq_key_map[min_freq].begin());

  return SUCCESS;
}

Status HeatSeparatorLfu::Get(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  if (this->key_freq_iter_map.find(key) == this->key_freq_iter_map.end())
    return ERROR;
  
  auto& item = this->key_freq_iter_map[key];
  this->freq_key_map[item.first].erase(item.second);
  if (this->freq_key_map[item.first].empty() 
      && 
      item.first == this->min_freq)
    this->min_freq++;
  item.first++;
  this->freq_key_map[item.first].push_front(key);
  item.second = this->freq_key_map[item.first].begin();
  this->key_freq_iter_map[key] = item;

  return SUCCESS;
}

bool HeatSeparatorLfu::IsHotKey(const std::string& key)
{
  return true;
}

Status HeatSeparatorLfu::GetHotKeys(std::vector<std::string>& hot_keys)
{
  // 从最高频向低频遍历
  for (auto freq = capacity; freq >= 1; freq--) 
  {
    if (this->freq_key_map.find(freq) == this->freq_key_map.end()) 
      continue;
    for (auto& k : this->freq_key_map[freq]) 
    {
      hot_keys.push_back(k);
    }
  }

  return SUCCESS;
}

void HeatSeparatorLfu::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== LFU INFO =====" << std::endl;

  

  std::cout << std::endl;
}

}