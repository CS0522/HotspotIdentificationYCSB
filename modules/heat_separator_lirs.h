#ifndef _HEAT_SEPARATOR_LIRS_H_
#define _HEAT_SEPARATOR_LIRS_H_

#include "separator.h"

#include <list>
#include <unordered_map>
#include <vector>
#include <string>
#include <algorithm>
#include <cassert>

namespace module
{

enum LIRSType 
{
    LIR = 0,      // 热数据块
    HIR,          // 驻留冷数据块
    NHIR          // 非驻留冷数据块
};

struct LIRSNode 
{
  std::string key;
  LIRSType type;
  // 在栈S中的位置
  std::list<LIRSNode*>::iterator stack_pos;
  // 在队列Q中的位置
  std::list<LIRSNode*>::iterator queue_pos;

  LIRSNode(const std::string& k, LIRSType t) 
      : key(k), type(t) {}
};

class HeatSeparatorLIRS : public HeatSeparator
{
private:
  // LIRS栈（访问历史）
  std::list<LIRSNode*> stack_s_;
  // 驻留HIR队列
  std::list<LIRSNode*> queue_q_;
  // Key到节点映射
  std::unordered_map<std::string, LIRSNode*> key_node_map_;
  
  size_t cache_size_;     // 总容量
  size_t s_size_;         // LIR块容量（99%）
  size_t q_size_;         // HIR块容量（1%）
  size_t used_size_ = 0;  // 当前使用容量

public:
  HeatSeparatorLIRS(size_t total_capacity); 
  ~HeatSeparatorLIRS();

  Status Put(const std::string& key);
  Status Get(const std::string& key);

  bool IsHotKey(const std::string& key) { return false; }
  Status GetHotKeys(std::vector<std::string>& hot_keys);

  void Display() {}

private:
    // @brief 淘汰队列尾部节点
    void FreeOne();
    // @brief 移动节点到栈顶
    void MoveToStackTop(LIRSNode* node);
    // @brief 移动节点到队列头部
    void MoveToQueueFront(LIRSNode* node);
    // @brief 从队列移除节点
    void RemoveFromQueue(LIRSNode* node);
    // @brief 栈底LIR降级为HIR
    void DemoteBottomLIR();
    // @brief 栈剪枝，移除底部非LIR块
    void PruneStack();
};

}

#endif