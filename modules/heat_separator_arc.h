#ifndef _HEAT_SEPARATOR_ARC_H_
#define _HEAT_SEPARATOR_ARC_H_

#include "separator.h"

#include <list>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <cassert>

namespace module
{

class HeatSeparatorARC : public HeatSeparator
{
private:

public:
  HeatSeparatorARC(size_t total_capacity); 

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key) { return false; }
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display() {}

private:

};

}

#endif