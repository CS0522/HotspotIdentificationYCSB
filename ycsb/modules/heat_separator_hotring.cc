#include "heat_separator_hotring.h"
#include <iomanip>

namespace module
{

HeatSeparatorHotRing::HeatSeparatorHotRing()
{
  std::cout << "Hot Ring Heat Separator is initialized" << std::endl;
}

Status HeatSeparatorHotRing::Put(const std::string& key)
{
}

Status HeatSeparatorHotRing::Get(const std::string& key)
{
}

bool HeatSeparatorHotRing::IsHotKey(const std::string& key)
{
}

Status HeatSeparatorHotRing::GetHotKeys(std::vector<std::string>& hot_keys)
{
  return SUCCESS;
}

void HeatSeparatorHotRing::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== Hot Ring INFO =====" << std::endl;

  std::cout << std::endl;
}

}