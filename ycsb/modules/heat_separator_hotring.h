#ifndef _HEAT_SEPARATOR_HOTRING_H_
#define _HEAT_SEPARATOR_HOTRING_H_

#include "separator.h"
#include <chrono>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace module
{

/**
 * 小根堆实现 Top-K
 */
class HeatSeparatorHotRing : public HeatSeparator
{
private:
  

public:
  HeatSeparatorHotRing();

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif