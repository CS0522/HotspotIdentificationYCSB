#ifndef _HEAT_SEPARATOR_HEAP_H_
#define _HEAT_SEPARATOR_HEAP_H_

// NO USED

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
class HeatSeparatorHeap : public HeatSeparator
{
private:
  struct HeapEntry
  {
    std::string key;
    uint32_t frequency;
    // 比较函数：频率小的元素在堆顶
    bool operator<(const HeapEntry& other) const 
    {
        return frequency > other.frequency;
    }
  };
  // top k
  size_t k;
  // threshold
  uint32_t freq_threshold;
  // 小根堆
  std::priority_queue<HeapEntry> min_heap;
  std::unordered_map<std::string, uint32_t> key_freq_map;
  // 额外维护小根堆中的元素
  std::unordered_set<std::string> hot_keys;

  Status UpdateAccessFreq(const std::string& key);
  // 当堆中元素需要更新频率，则需要对堆进行重建，这里开销很大
  Status RebuildHeapWithUpdatedKey(const std::string&key, const uint32_t key_curr_freq);

public:
  HeatSeparatorHeap(const size_t k, const uint32_t freq_threshold);

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key);
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display();
};

}

#endif