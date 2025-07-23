#include "heat_separator_s3_fifo.h"

namespace module
{
HeatSeparatorS3FIFO::HeatSeparatorS3FIFO(size_t total_capacity)
 : total_capacity_(total_capacity), 
   small_queue_(static_cast<size_t>(total_capacity * 0.1)),
   main_queue_(static_cast<size_t>(total_capacity * 0.9))
{
  std::cout << "S3-FIFO Heat Separator is initialized " << std::endl;
  std::cout << "  Capacity: " << this->total_capacity_ << std::endl;

  this->algorithm_name = "s3_fifo";
}

Status HeatSeparatorS3FIFO::Put(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  auto now_timestamp = time(nullptr);

  // Key 存在时
  auto iter = this->key_map_.find(key);
  if (iter != this->key_map_.end())
  {
    KeyNode& node = *(iter->second);
    node.frequency = std::min((size_t)3, (node.frequency) + 1);
    node.last_access_timestamp = now_timestamp;
    return SUCCESS;
  }

  // 检查 Ghost 队列
  size_t hash_val = GhostHash(key);
  auto ghost_iter = this->ghost_map_.find(hash_val);
  if (ghost_iter != this->ghost_map_.end())
  {
    // 直接进入 main 队列
    ghost_queue_.erase(ghost_iter->second);
    ghost_map_.erase(hash_val);
    this->main_queue_.data.emplace_back(key);
    this->key_map_[key] = std::prev(this->main_queue_.data.end());
    
    if (this->main_queue_.data.size() > this->main_queue_.max_size)
      this->EvictFromMainQueue();
    return SUCCESS;
  }

  // 新 Key 插入 Small 队列
  this->small_queue_.data.emplace_front(key);
  this->key_map_[key] = this->small_queue_.data.begin();
  // 小型队列满时触发驱逐
  if (this->small_queue_.data.size() > this->small_queue_.max_size)
    this->EvictFromSmallQueue();
  return SUCCESS;
}

Status HeatSeparatorS3FIFO::Get(const std::string& key)
{
  auto iter = this->key_map_.find(key);
  if (iter == this->key_map_.end())
    return ERROR;

  KeyNode& node = *(iter->second);
  node.frequency = std::min((size_t)3, (node.frequency) + 1);
  node.last_access_timestamp = time(nullptr);
  
  return SUCCESS;
}

Status HeatSeparatorS3FIFO::GetHotKeys(std::vector<std::string>& hot_keys)
{
  for (auto& node : this->main_queue_.data) {
    if (node.frequency >= 1)
      hot_keys.push_back(node.key);
  }
  return SUCCESS;
}

size_t HeatSeparatorS3FIFO::GhostHash(const std::string& key)
{
  return std::hash<std::string>{}(key);
}

void HeatSeparatorS3FIFO::EvictFromSmallQueue()
{
  while (!this->small_queue_.data.empty())
  {
    KeyNode& node = this->small_queue_.data.back();

    // 低频移入幽灵队列
    if (node.frequency == 0)
    {
      size_t hash_val = this->GhostHash(node.key);
      this->ghost_queue_.emplace_front(hash_val);
      this->ghost_map_[hash_val] = this->ghost_queue_.begin();
    }
    // 高频移入 main 队列
    else
      this->PromoteToMainQueue(node.key);

    this->key_map_.erase(node.key);
    this->small_queue_.data.pop_back();
  }
}

void HeatSeparatorS3FIFO::EvictFromMainQueue()
{
  auto iter = this->main_queue_.data.begin();
  while (iter != this->main_queue_.data.end())
  {
    // 找到可驱逐的低频Key
    if (iter->frequency == 0) 
    {
      this->key_map_.erase(iter->key);
      iter = this->main_queue_.data.erase(iter);
      return;
    }
    // 重置频率
    else
    {
      iter->frequency--;
      this->main_queue_.data.splice(main_queue_.data.end(), main_queue_.data, iter++);
    }
  }
}

void HeatSeparatorS3FIFO::PromoteToMainQueue(const std::string& key)
{
  auto iter = this->key_map_[key];
  iter->is_in_main_queue = true;

  this->main_queue_.data.splice(main_queue_.data.end(), small_queue_.data, iter);
        
  if (main_queue_.data.size() > main_queue_.max_size)
    this->EvictFromMainQueue();
}

}