#include "heat_separator_lirs.h"

namespace module
{
HeatSeparatorLIRS::HeatSeparatorLIRS(size_t total_capacity)
: cache_size_(total_capacity),
  s_size_(static_cast<size_t>(total_capacity * 0.99)),
  q_size_(std::max(static_cast<size_t>(total_capacity * 0.01), static_cast<size_t>(1)))
{
  std::cout << "LIRS Heat Separator is initialized " << std::endl;
  std::cout << "  Capacity: " << this->cache_size_ << std::endl;

  this->algorithm_name = "lirs";
}

HeatSeparatorLIRS::~HeatSeparatorLIRS()
{
  for (auto& pair : key_node_map_) 
    delete pair.second;
}

// 插入或更新Key访问
Status HeatSeparatorLIRS::Put(const std::string& key) 
{
  // 已存在则更新访问状态
  if (this->key_node_map_.find(key) != this->key_node_map_.end()) 
    return this->Get(key);

  // 缓存满时淘汰数据
  while (used_size_ >= cache_size_ || queue_q_.size() >= q_size_)
    FreeOne();
    
  // 创建新节点（初始为LIR状态）
  LIRSNode* new_node = new LIRSNode(key, LIR);
  this->key_node_map_[key] = new_node;
  
  // 加入栈顶
  this->stack_s_.push_front(new_node);
  new_node->stack_pos = stack_s_.begin();
  this->used_size_++;
  
  // 超过LIR容量时降级为HIR
  if (used_size_ > s_size_) 
  {
    new_node->type = HIR;
    queue_q_.push_front(new_node);
    new_node->queue_pos = queue_q_.begin();
  }

  return SUCCESS;
}

Status HeatSeparatorLIRS::Get(const std::string& key) 
{
  auto iter = this->key_node_map_.find(key);
  if ( iter == this->key_node_map_.end())
    return ERROR;
    
  LIRSNode* node = iter->second;
    
  switch (node->type)
  {
    case LIR:
      this->MoveToStackTop(node);
      if (node->stack_pos == std::prev(stack_s_.end())) 
        this->PruneStack();
      return SUCCESS;
          
    case HIR:
      if (node->stack_pos != stack_s_.end()) 
      {
        // 升级为LIR并降级栈底节点
        node->type = LIR;
        this->MoveToStackTop(node);
        this->RemoveFromQueue(node);
        this->DemoteBottomLIR();
        this->PruneStack();
      }
      else 
      {
        // 加入栈顶并更新队列位置
        stack_s_.push_front(node);
        node->stack_pos = stack_s_.begin();
        this->MoveToQueueFront(node);
      }
      return SUCCESS;
          
    case NHIR:
      // 重新激活非驻留节点
      while (used_size_ >= cache_size_ || queue_q_.size() >= q_size_)
        FreeOne();
      if (node->stack_pos != stack_s_.end()) 
      {
        node->type = LIR;
        MoveToStackTop(node);
        DemoteBottomLIR();
        used_size_++;
      }
      else 
      {
        // 异常情况处理
        node->type = HIR;
        stack_s_.push_front(node);
        node->stack_pos = stack_s_.begin();
        queue_q_.push_front(node);
        node->queue_pos = queue_q_.begin();
        used_size_++;
      }
      PruneStack();
      return SUCCESS;
  }
  return ERROR;
}

// 获取所有热Keys (LIR块)
Status HeatSeparatorLIRS::GetHotKeys(std::vector<std::string>& hot_keys)
{
    for (auto node : stack_s_)
    {
      if (node->type == LIR) 
        hot_keys.push_back(node->key);
    }
    return SUCCESS;
}


void HeatSeparatorLIRS::FreeOne() 
{
  if (this->queue_q_.empty()) return;
    
  LIRSNode* node = queue_q_.back();
  queue_q_.pop_back();
  node->queue_pos = queue_q_.end();
  
  if (node->type == HIR) 
  {
    node->type = NHIR;
    used_size_--;
  }
  
  // 清理孤立节点
  if (node->stack_pos == stack_s_.end()) 
  {
    this->key_node_map_.erase(node->key);
    delete node;
  }
}

void HeatSeparatorLIRS::MoveToStackTop(LIRSNode* node) 
{
  stack_s_.erase(node->stack_pos);
  stack_s_.push_front(node);
  node->stack_pos = stack_s_.begin();
}

void HeatSeparatorLIRS::MoveToQueueFront(LIRSNode* node) 
{
  queue_q_.erase(node->queue_pos);
  queue_q_.push_front(node);
  node->queue_pos = queue_q_.begin();
}

void HeatSeparatorLIRS::RemoveFromQueue(LIRSNode* node) 
{
  if (node->queue_pos != queue_q_.end()) 
  {
    queue_q_.erase(node->queue_pos);
    node->queue_pos = queue_q_.end();
  }
}

void HeatSeparatorLIRS::DemoteBottomLIR() 
{
  if (stack_s_.empty()) return;
  
  LIRSNode* bottom = stack_s_.back();
  if (bottom->type == LIR) 
  {
    bottom->type = HIR;
    queue_q_.push_front(bottom);
    bottom->queue_pos = queue_q_.begin();
  }
}

void HeatSeparatorLIRS::PruneStack() {
  while (!stack_s_.empty()) 
  {
    LIRSNode* bottom = stack_s_.back();
    if (bottom->type == LIR) break;
    
    stack_s_.pop_back();
    bottom->stack_pos = stack_s_.end();
    
    // 清理无引用节点
    if (bottom->type == NHIR && bottom->queue_pos == queue_q_.end()) 
    {
      this->key_node_map_.erase(bottom->key);
      delete bottom;
    }
  }
}

}