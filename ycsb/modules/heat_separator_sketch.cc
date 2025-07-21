#include "heat_separator_sketch.h"
#include <iomanip>

namespace module
{

CountMinSketch::CountMinSketch(const double e, const double delta, const size_t t)
  : threshold(t)
{
  this->width = static_cast<uint32_t>(std::ceil(2.0 / e));
  this->depth = static_cast<uint32_t>(std::ceil(std::log(1.0 / delta)));
  this->counter_table.resize(this->depth, std::vector<uint64_t>(width, 0));

  // 初始化哈希函数（独立种子减少冲突）
  for (size_t i = 0; i < this->depth; i++) 
  {
    this->hash_funcs.push_back([i](const std::string& key) 
    {
      size_t seed = std::hash<std::string>{}(key);
      // 黄金比例扰动
      seed += i * 0x9e3779b9;
      return seed;
    });
  }

  std::cout << "Count Min Sketch: " << std::endl;
  std::cout << "  e: " << e << ", delta: " << delta << ", threshold: " << t << std::endl;
  std::cout << "  width: " << this->width << ", depth: " << this->depth << std::endl;
}

Status CountMinSketch::UpdateCounter(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  for (size_t i = 0; i < depth; ++i) 
  {
    size_t hash_val = this->hash_funcs[i](key);
    size_t col = hash_val % this->width;
    this->counter_table[i][col] += 1;
  }
  return SUCCESS;
}

Status CountMinSketch::Put(const std::string& key)
{
  return this->UpdateCounter(key);
}

Status CountMinSketch::Get(const std::string& key)
{
  return this->UpdateCounter(key);
}

bool CountMinSketch::IsHotKey(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  uint64_t min_count = static_cast<uint64_t>(INT64_MAX);
  for (size_t i = 0; i < this->depth; ++i)
  {
    size_t hash_val = this->hash_funcs[i](key);
    size_t col = hash_val % this->width;
    min_count = std::min(min_count, this->counter_table[i][col]);
  }
  return (min_count >= this->threshold);
}

uint64_t CountMinSketch::GetKeyCount(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  uint64_t min_count = static_cast<uint64_t>(INT64_MAX);
  for (size_t i = 0; i < this->depth; ++i)
  {
    size_t hash_val = this->hash_funcs[i](key);
    size_t col = hash_val % this->width;
    min_count = std::min(min_count, this->counter_table[i][col]);
  }
  return min_count;
}

void CountMinSketch::Display()
{
  std::cout << "Count Min Sketch has no info to display" << std::endl;
}


LRUCache::LRUCache(const size_t c) : capacity(c)
{
  std::cout << "LRU Window Size: " << this->capacity << std::endl;
}

void LRUCache::SetWindowSize(const size_t size)
{
  this->capacity = size;
  std::cout << "LRU Window is resized as: " << this->capacity << std::endl;
}

Status LRUCache::Evict()
{
  auto last_key = this->key_access_list.back();
  this->key_cache_map.erase(last_key);
  this->key_access_list.pop_back();
  
  return SUCCESS;
}

Status LRUCache::Put(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  // 如果存在以前访问，则先删除
  auto map_iter = this->key_cache_map.find(key);
  if (map_iter != this->key_cache_map.end())
  {
    this->key_access_list.erase(map_iter->second);
  }
  else if (this->key_cache_map.size() >= this->capacity)
  {
    this->Evict();
  }
  // 队头插入
  this->key_access_list.push_front(key);
  this->key_cache_map[key] = this->key_access_list.begin();

  return SUCCESS;
}

Status LRUCache::Get(const std::string& key)
{
  return this->Put(key);
}

Status LRUCache::GetAllKeys(std::vector<std::string>& keys)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  for (auto const& key : this->key_access_list)
    keys.push_back(key);
  
  return SUCCESS;
}

bool LRUCache::IsContain(const std::string& key)
{
  auto map_iter = this->key_cache_map.find(key);
  return (map_iter != this->key_cache_map.end());
}

void LRUCache::Display()
{
  std::cout << "  LRU Window: " << std::endl;
  std::vector<std::string> lru_all_keys;
  this->GetAllKeys(lru_all_keys);
  for (auto const& key : lru_all_keys)
    std::cout << "    " << key << std::endl;
}


HeatSeparatorSketch::HeatSeparatorSketch(const size_t window_size, 
                      const double e, const double delta, const size_t t, const bool enable_lru)
  : lru_window(window_size), count_min_sketch(e, delta, t), enable_lru(enable_lru)
{
  std::cout << "Count-Min Sketch Heat Separator is initialized" << std::endl;

  this->algorithm_name = "sketch";

  if (!enable_lru)
    this->lru_window.SetWindowSize(UINT64_MAX);
}

Status HeatSeparatorSketch::Put(const std::string& key)
{
  Status res1 = this->lru_window.Put(key);
  Status res2 = this->count_min_sketch.Put(key);
  if (res1 == 0 && res2 == 0)
    return SUCCESS;
  else
    return ERROR;
}

Status HeatSeparatorSketch::Get(const std::string& key)
{
  Status res1 = this->lru_window.Get(key);
  Status res2 = this->count_min_sketch.Get(key);
  if (res1 == 0 && res2 == 0)
    return SUCCESS;
  else
    return ERROR;
}

bool HeatSeparatorSketch::IsHotKey(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  
  // 首先查找时间窗口，存在则继续查询 CMS
  if (this->lru_window.IsContain(key))
    if (this->count_min_sketch.IsHotKey(key))
      return true;
  
  return false;
}

Status HeatSeparatorSketch::GetHotKeys(std::vector<std::string>& hot_keys)
{
  // 首先获取 LRU 中的所有 Keys，然后查询 CMS 是否频率大于阈值
  std::vector<std::string> lru_all_keys;
  this->lru_window.GetAllKeys(lru_all_keys);

  for (auto const& lru_key : lru_all_keys)
  {
    if (this->count_min_sketch.IsHotKey(lru_key))
      hot_keys.push_back(lru_key);
  }

  return SUCCESS;
}

void HeatSeparatorSketch::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== Sketch INFO =====" << std::endl;
  
  // this->lru_window.Display();
  // this->count_min_sketch.Display();

  // std::cout << "==========" << std::endl;

  std::vector<std::string> lru_all_keys;
  this->lru_window.GetAllKeys(lru_all_keys);
  std::vector<uint64_t> lru_all_keys_count;
  for (auto const& lru_key : lru_all_keys)
  {
    std::cout << "  lru-key: " << std::setw(20) << lru_key << ", count-min: " 
              << this->count_min_sketch.GetKeyCount(lru_key) << std::endl;
  }

  std::cout << std::endl;
}

}