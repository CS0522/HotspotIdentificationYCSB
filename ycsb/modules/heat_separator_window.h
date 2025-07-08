#ifndef _HEAT_SEPARATOR_WINDOW_H_
#define _HEAT_SEPARATOR_WINDOW_H_

#include "separator.h"
#include <chrono>
#include <unordered_map>
#include <deque>

namespace module
{

/**
 * 
 */
class HeatSeparatorWindow : public HeatSeparator
{
private:
  struct AccessEntry
  {
    std::string key;
    // timestamp
    std::chrono::steady_clock::time_point timestamp;
  };

  // 采样窗口大小
  std::chrono::milliseconds window_size;
  uint32_t threshold;
  // Key 访问计数
  std::unordered_map<std::string, uint32_t> key_access_count;
  // Key 访问记录队列
  std::deque<AccessEntry> key_access_queue;

  Status RecordAccess(const std::string& key);

public:
  HeatSeparatorWindow(const std::chrono::milliseconds w_s, const uint32_t t);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif