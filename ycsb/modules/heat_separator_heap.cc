#include "heat_separator_heap.h"
#include <iomanip>

// NO USED

namespace module
{

HeatSeparatorHeap::HeatSeparatorHeap(const size_t k_, const uint32_t f_t)
  : k(k_), freq_threshold(f_t)
{
  std::cout << "Min Heap Top-K Heat Separator is initialized" << std::endl;
}

Status HeatSeparatorHeap::UpdateAccessFreq(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  this->key_freq_map[key]++;
  auto key_curr_freq = this->key_freq_map[key];

  if (key_curr_freq >= freq_threshold)
  {
    // 1. 已在堆中
    if (this->hot_keys.find(key) != this->hot_keys.end())
      return this->RebuildHeapWithUpdatedKey(key, key_curr_freq);

    // 2. 未在堆中
    if (this->min_heap.size() < this->k)
    {
      this->min_heap.push({key, key_curr_freq});
      this->hot_keys.insert(key);
    }
    else if (key_curr_freq > this->min_heap.top().frequency)
    {
      hot_keys.erase(min_heap.top().key);
      min_heap.pop();
      min_heap.push({key, key_curr_freq});
      hot_keys.insert(key);
    }
  }

  return SUCCESS;
}

Status HeatSeparatorHeap::RebuildHeapWithUpdatedKey(const std::string&key, const uint32_t key_curr_freq)
{
  std::vector<HeapEntry> heap_entries;
  while (!min_heap.empty()) 
  {
    auto entry = min_heap.top();
    min_heap.pop();
    if (entry.key == key) 
      entry.frequency = key_curr_freq;
    heap_entries.push_back(entry);
  }
  // 重新入堆
  for (auto& entry : heap_entries) {
      min_heap.push(entry);
  }
  return SUCCESS;
}

Status HeatSeparatorHeap::Put(const std::string& key)
{
  return this->UpdateAccessFreq(key);
}

Status HeatSeparatorHeap::Get(const std::string& key)
{
  return this->UpdateAccessFreq(key);
}

bool HeatSeparatorHeap::IsHotKey(const std::string& key)
{
  return (this->hot_keys.find(key) != this->hot_keys.end());
}

Status HeatSeparatorHeap::GetHotKeys(std::vector<std::string>& hot_keys)
{
  hot_keys.assign(this->hot_keys.begin(), this->hot_keys.end());
  return SUCCESS;
}

void HeatSeparatorHeap::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== Heap INFO =====" << std::endl;
  
  std::cout << "Hot Keys: " << std::endl;
  std::vector<std::string> hot_keys;
  this->GetHotKeys(hot_keys);
  for (auto const& key : hot_keys)
    std::cout << "  Key: " << key << ", count: " << this->key_freq_map.find(key)->second << std::endl;
  
  std::cout << ">>>>><<<<<" << std::endl;
  std::cout << "All Keys: " << std::endl;
  for (auto map_iter = this->key_freq_map.begin(); map_iter != this->key_freq_map.end(); map_iter++)
    std::cout << "  Key: " << map_iter->first << ", count: " << map_iter->second << std::endl;

  std::cout << std::endl;
}

}