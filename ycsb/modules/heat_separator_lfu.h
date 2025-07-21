#ifndef _HEAT_SEPARATOR_LFU_H_
#define _HEAT_SEPARATOR_LFU_H_

#include "separator.h"
#include <list>
#include <unordered_map>

namespace module
{

/**
 * 最后得到的 Cache Queue 中的数据即为识别的热数据
 */
class HeatSeparatorLfu : public HeatSeparator
{
private:
  size_t capacity;
  size_t min_freq;
  // 频率到 key 映射，可能会有同频
  std::unordered_map<size_t, std::list<std::string>> freq_key_map;
  // key 到频率和迭代器映射
  std::unordered_map<std::string, std::pair<size_t, std::list<std::string>::iterator>> key_freq_iter_map;

public:
  HeatSeparatorLfu(const size_t capacity, const size_t min_freq);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif