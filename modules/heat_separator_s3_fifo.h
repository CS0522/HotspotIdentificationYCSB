#ifndef _HEAT_SEPARATOR_S3_FIFO_H_
#define _HEAT_SEPARATOR_S3_FIFO_H_

#include "separator.h"

#include <list>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <cassert>
#include <ctime>

namespace module
{

class HeatSeparatorS3FIFO : public HeatSeparator
{
private:
  // 队列节点
  struct KeyNode
  {
    std::string key;
    size_t frequency;
    bool is_in_main_queue;
    time_t last_access_timestamp;

    KeyNode(std::string k)
    : key(k), frequency(0), is_in_main_queue(false), last_access_timestamp(time(nullptr)) {}
  };
  // 幽灵队列节点
  struct GhostNode
  {
    size_t hash;
    time_t insert_timestamp;
    
    GhostNode(size_t h): hash(h), insert_timestamp(time(nullptr)) {}
  };
  // 队列配置
  struct S3Queue
  {
    size_t max_size;
    std::list<KeyNode> data;
    S3Queue(size_t size) : max_size(size) {}
  };

  // 3 个队列
  S3Queue small_queue_;
  S3Queue main_queue_;
  std::list<GhostNode> ghost_queue_;

  std::unordered_map<std::string, std::list<KeyNode>::iterator> key_map_;
  std::unordered_map<size_t, std::list<GhostNode>::iterator> ghost_map_;

  size_t total_capacity_;

  size_t GhostHash(const std::string& key);
  void EvictFromSmallQueue();
  void EvictFromMainQueue();
  void PromoteToMainQueue(const std::string& key);

public:
  HeatSeparatorS3FIFO(size_t total_capacity); 

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key) { return false; }
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display() {}
};

}

#endif