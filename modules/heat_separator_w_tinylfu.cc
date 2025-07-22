#include "heat_separator_w_tinylfu.h"
#include <iomanip>

namespace module
{

HeatSepratorWTinyLFU::HeatSepratorWTinyLFU(const size_t capacity)
  : w_tinyflu(capacity), capacity(capacity)
{
  std::cout << "External W-TinyLFU Heat Separator is initialized" << std::endl;
  std::cout << "  Total Capacity: " << this->capacity << std::endl;

  this->algorithm_name = "w_tinylfu";
}

Status HeatSepratorWTinyLFU::UpdateAccessKey(const std::string& key)
{
  return (this->w_tinyflu.Put(key, "0") ? SUCCESS : ERROR);
}

Status HeatSepratorWTinyLFU::Put(const std::string& key)
{
  return this->UpdateAccessKey(key);
}

Status HeatSepratorWTinyLFU::Get(const std::string& key)
{
  return this->UpdateAccessKey(key);
}

bool HeatSepratorWTinyLFU::IsHotKey(const std::string& key)
{
  return false;
}

Status HeatSepratorWTinyLFU::GetHotKeys(std::vector<std::string>& hot_keys)
{
  return (this->w_tinyflu.GetHotKeys(hot_keys) ? SUCCESS : ERROR);
}

void HeatSepratorWTinyLFU::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== W-TinyLFU INFO =====" << std::endl;
  
  

  std::cout << std::endl;
}

}