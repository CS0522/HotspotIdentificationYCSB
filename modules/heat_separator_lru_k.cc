#include "heat_separator_lru_k.h"
#include <iomanip>

namespace module
{

HeatSeparatorLruK::HeatSeparatorLruK(const uint32_t k_, const size_t c_)
  : k(k_), capacity(c_)
{
  std::cout << "Lru-K Heat Separator is initialized, k = " << k << ", queue capacity = " << capacity << std::endl;

  this->algorithm_name = "lruk";
}

Status HeatSeparatorLruK::Put(const std::string& key)
{
  if (this->capacity <= 0)
    return NO_SPACE;

  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  auto map_iter = this->key_cache_map.find(key);
  // 已存在条目
  if (map_iter != this->key_cache_map.end())
  {
    LruKCacheEntry& entry = map_iter->second;
    entry.access_count += 1;

    // 历史访问次数到达阈值，迁移队列
    if (entry.access_count == this->k) 
    {
      this->history_queue.erase(entry.history_iter);
      this->cache_queue.push_front(key);
      entry.cache_iter = this->cache_queue.begin();
    }
    // 访问次数超过 k，则肯定在缓存队列中但不是队头
    else if (entry.access_count > this->k) 
    {
      this->cache_queue.erase(entry.cache_iter);
      this->cache_queue.push_front(key);
      entry.cache_iter = this->cache_queue.begin();
    }
    return SUCCESS;
  }

  if (this->key_cache_map.size() >= this->capacity)
    this->Evict();

  // 插入新条目到历史队列
  this->history_queue.push_front(key);
  // 暂未插入到缓存队列
  LruKCacheEntry new_entry{key, 1, this->history_queue.begin(), std::list<std::string>::iterator()};
  this->key_cache_map[key] = new_entry;

  return SUCCESS;
}

Status HeatSeparatorLruK::Get(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  auto map_iter = this->key_cache_map.find(key);
  
  if (map_iter == this->key_cache_map.end())
    return NOT_FOUND;
  
  // 更新访问计数
  LruKCacheEntry& entry = map_iter->second;
  entry.access_count += 1;

  // 历史访问次数到达阈值，迁移队列
  if (entry.access_count == this->k) 
  {
    this->history_queue.erase(entry.history_iter);
    this->cache_queue.push_front(key);
    entry.cache_iter = this->cache_queue.begin();
  }
  // 访问次数超过 k，则肯定在缓存队列中但不是队头
  else if (entry.access_count > this->k) 
  {
    this->cache_queue.erase(entry.cache_iter);
    this->cache_queue.push_front(key);
    entry.cache_iter = this->cache_queue.begin();
  }

  return SUCCESS;
}

bool HeatSeparatorLruK::IsHotKey(const std::string& key)
{
  auto find_iter = std::find(this->cache_queue.begin(), this->cache_queue.end(), key);
  return (find_iter != this->cache_queue.end());
}

Status HeatSeparatorLruK::Evict()
{
  std::string key_to_remove;
  // 优先淘汰历史队列尾部
  if (!history_queue.empty()) 
  {
    key_to_remove = this->history_queue.back();
    this->history_queue.pop_back();
  } 
  // 若历史队列空，淘汰缓存队列尾部
  else if (!cache_queue.empty()) 
  {
    key_to_remove = cache_queue.back();
    this->cache_queue.pop_back();
  }
  this->key_cache_map.erase(key_to_remove);
  return SUCCESS;
}

Status HeatSeparatorLruK::GetHotKeys(std::vector<std::string>& hot_keys)
{
  hot_keys = std::vector<std::string>(this->cache_queue.begin(), this->cache_queue.end());
  return SUCCESS;
}

void HeatSeparatorLruK::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== LRU-K INFO =====" << std::endl;
  
  std::cout << "History Queue: " << std::endl;
  for (auto history_iter = this->history_queue.begin(); history_iter != this->history_queue.end(); history_iter++)
  {
    std::cout << "  " << std::setw(20) << *history_iter;
    auto map_iter = this->key_cache_map.find(*history_iter);
    if (map_iter != this->key_cache_map.end())
      std::cout << ", " << std::setw(10) << map_iter->second.access_count;
    std::cout << std::endl;
  }

  std::cout << "Cache Queue: " << std::endl;
  for (auto cache_iter = this->cache_queue.begin(); cache_iter != this->cache_queue.end(); cache_iter++)
  {
    std::cout << "  " << std::setw(20) << *cache_iter;
    auto map_iter = this->key_cache_map.find(*cache_iter);
    if (map_iter != this->key_cache_map.end())
      std::cout << ", " << std::setw(10) << map_iter->second.access_count;
    std::cout << std::endl;
  }

  std::cout << std::endl;
}

}