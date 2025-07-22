#include "heat_separator_lru.h"
#include <iomanip>

namespace module
{

HeatSeparatorLru::HeatSeparatorLru(const size_t c_)
  : capacity(c_)
{
  std::cout << "Lru Heat Separator is initialized, queue capacity = " << capacity << std::endl;

  this->algorithm_name = "lru";
}

Status HeatSeparatorLru::Put(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  // 如果已存在该条目
  if (this->key_cache_map.find(key) != this->key_cache_map.end())
  {
    this->cache_queue.splice(this->cache_queue.end(), this->cache_queue, this->key_cache_map[key]);
    return SUCCESS;
  }
  // 缓存满时淘汰头部
  if (this->cache_queue.size() >= this->capacity)
  {
    auto old_key = this->cache_queue.front();
    this->key_cache_map.erase(old_key);
    this->cache_queue.pop_front();
  }

  this->cache_queue.push_back(key);
  // 指向尾部
  this->key_cache_map[key] = std::prev(this->cache_queue.end());
  return SUCCESS;
}

Status HeatSeparatorLru::Get(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  if (this->key_cache_map.find(key) == this->key_cache_map.end())
    return ERROR;
  // 移动到链表尾部
  this->cache_queue.splice(this->cache_queue.end(), this->cache_queue, this->key_cache_map[key]);
  return SUCCESS;
}

bool HeatSeparatorLru::IsHotKey(const std::string& key)
{
  auto find_iter = std::find(this->cache_queue.begin(), this->cache_queue.end(), key);
  return (find_iter != this->cache_queue.end());
}

Status HeatSeparatorLru::GetHotKeys(std::vector<std::string>& hot_keys)
{
  
  for (auto iter = this->cache_queue.rbegin(); iter != this->cache_queue.rend(); iter++) 
    hot_keys.push_back(*iter);
  return ((hot_keys.size() == this->cache_queue.size()) ? SUCCESS : ERROR);
}

void HeatSeparatorLru::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== LRU INFO =====" << std::endl;

  std::cout << "Cache Queue: " << std::endl;
  for (auto cache_iter = this->cache_queue.rbegin(); cache_iter != this->cache_queue.rend(); cache_iter++)
  {
    std::cout << "  " << std::setw(20) << *cache_iter;
    std::cout << std::endl;
  }

  std::cout << std::endl;
}

}