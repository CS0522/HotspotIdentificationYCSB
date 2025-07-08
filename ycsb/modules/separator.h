#ifndef _SEPARATOR_H_
#define _SEPARATOR_H_

#include <iostream>
#include <vector>
#include <algorithm>
#include <atomic>
#include <mutex>

namespace module
{

enum Status
{
  SUCCESS,
  ERROR,
  NOT_FOUND,
  NO_SPACE
};

class Separator
{
protected:
  std::mutex separator_mtx_;

public:
  virtual Status Put(const std::string& key) = 0;
  virtual Status Get(const std::string& key) = 0;

  // @brief 返回是否为热数据
  virtual bool IsHotKey(const std::string& key) = 0;
  // @brief 返回所有热数据
  virtual Status GetHotKeys(std::vector<std::string>& hot_keys) = 0;

  virtual void Display() = 0;
};

/**
 * 声明 冷热识别算法 组件基类
 */
class HeatSeparator : public Separator
{
protected:

public:
};

/**
 * 声明 大小分流 组件基类
 */
class SizeSeparator : public Separator
{
protected:

public:
};

}

#endif