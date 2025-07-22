#ifndef _HEAT_SEPARATOR_SKETCH_WINDOW_H_
#define _HEAT_SEPARATOR_SKETCH_WINDOW_H_

#include "separator.h"
#include <chrono>
#include <unordered_map>
#include <vector>
#include <list>
#include <functional>
#include <cmath>

namespace module
{

class CountMinSketch : public HeatSeparator
{
private:
  // 哈希函数数量
  uint32_t depth;
  // 计数器数组宽度
  uint32_t width;
  // 频率阈值
  size_t threshold;
  // d*w 二维计数器
  std::vector<std::vector<uint64_t>> counter_table;
  std::vector<std::function<size_t(const std::string&)>> hash_funcs;

  Status UpdateCounter(const std::string& key);

public:
  // e 控制误差范围，delta 控制误差概率
  CountMinSketch(const double e, const double delta, const size_t t);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  uint64_t GetKeyCount(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys) { return SUCCESS; };

  void Display();
};

class LRUCache : public HeatSeparator
{
private:
  size_t capacity;
  std::list<std::string> key_access_list;
  std::unordered_map<std::string, std::list<std::string>::iterator> key_cache_map;

  Status Evict();

public:
  LRUCache(const size_t c);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  // not needed in this class
  bool IsHotKey(const std::string& key) { return false; };
  // not needed in this class
  Status GetHotKeys(std::vector<std::string>& hot_keys) { return SUCCESS; };

  Status GetAllKeys(std::vector<std::string>& keys);
  bool IsContain(const std::string& key);

  void SetWindowSize(const size_t size);

  void Display();
};

/**
 * CMS + LRU，兼顾频繁度和时效性
 * 热 Key 需要满足：在时间窗口中、且 CMS 大于阈值
 */
class HeatSeparatorSketch : public HeatSeparator
{
private:
  // 时间窗口
  LRUCache lru_window;
  // CMS
  CountMinSketch count_min_sketch;
  bool enable_lru;

public:
  HeatSeparatorSketch(const size_t window_size, 
                      const double e, const double delta, const size_t t, 
                      const bool enable_lru);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif