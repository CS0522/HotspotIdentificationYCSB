#ifndef _HEAT_SEPARATOR_LRU_H_
#define _HEAT_SEPARATOR_LRU_H_

#include "separator.h"
#include <list>
#include <unordered_map>

namespace module
{

/**
 * 最后得到的 Cache Queue 中的数据即为识别的热数据
 */
class HeatSeparatorLru : public HeatSeparator
{
private:
  // 缓存队列容量
  size_t capacity;
  std::list<std::string> cache_queue;
  std::unordered_map<std::string, std::list<std::string>::iterator> key_cache_map;

public:
  HeatSeparatorLru(const size_t c_);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif