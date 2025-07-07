#ifndef _HEAT_SEPARATOR_LRU_K_H_
#define _HEAT_SEPARATOR_LRU_K_H_

#include "separator.h"
#include <list>
#include <unordered_map>

namespace module
{

/**
 * 最后得到的 Cache Queue 中的数据即为识别的热数据
 */
class HeatSeparatorLruK : public HeatSeparator
{
private:
  // Entry structure
  struct LruKCacheEntry
  {
    std::string key;
    // 该条目访问次数
    uint64_t access_count;

    // 历史队列迭代器
    std::list<std::string>::iterator history_iter;
    // 缓存队列迭代器
    std::list<std::string>::iterator cache_iter;
  };

  // 访问次数阈值
  uint32_t k;
  // 缓存队列容量
  uint32_t capacity;
  std::list<std::string> cache_queue;
  std::list<std::string> history_queue;
  // Key --> Lru-K Cache Entry
  std::unordered_map<std::string, LruKCacheEntry> key_cache_map;

  // @brief 淘汰数据
  Status Evict();

public:
  HeatSeparatorLruK(const uint32_t k_, const uint32_t c_);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);

  void Display();
};

}

#endif