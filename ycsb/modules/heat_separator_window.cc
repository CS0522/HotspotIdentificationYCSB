#include "heat_separator_window.h"
#include <iomanip>

namespace module
{

HeatSeparatorWindow::HeatSeparatorWindow(const std::chrono::milliseconds w_s, const uint32_t t)
  : window_size(w_s), threshold(t)
{
  std::cout << "Window Heat Separator is initialized" << std::endl;
}

Status HeatSeparatorWindow::RecordAccess(const std::string& key)
{
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

  // clean out-of-date records
  while (!this->key_access_queue.empty() &&
          (std::chrono::duration_cast<std::chrono::milliseconds>(
            now - key_access_queue.front().timestamp) > this->window_size))
  {
    auto old_record = this->key_access_queue.front();
    this->key_access_queue.pop_front();
    // map 清理机制，map 空间有限不能记录所有 key
    if (--key_access_count[old_record.key] == 0)
      this->key_access_count.erase(old_record.key);
  }

  // update key access records
  this->key_access_queue.push_back({key, now});
  this->key_access_count[key]++;

  return SUCCESS;
}

Status HeatSeparatorWindow::Put(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  std::cout << "Timestamp: " << std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count() << std::endl;
  return this->RecordAccess(key);
}

Status HeatSeparatorWindow::Get(const std::string& key)
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);
  std::cout << "Timestamp: " << std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count() << std::endl;
  return this->RecordAccess(key);
}

bool HeatSeparatorWindow::IsHotKey(const std::string& key)
{
  auto access_count = this->key_access_count[key];
  return access_count >= this->threshold;
}

Status HeatSeparatorWindow::GetHotKeys(std::vector<std::string>& hot_keys)
{
  for (auto map_iter = this->key_access_count.begin(); map_iter != this->key_access_count.end(); map_iter++)
  {
    if (map_iter->second >= this->threshold)
      hot_keys.push_back(map_iter->first);
  }
  return SUCCESS;
}

void HeatSeparatorWindow::Display()
{
  std::lock_guard<std::mutex> lock(this->separator_mtx_);

  std::cout << "===== Window INFO =====" << std::endl;
  
  std::cout << "Key Access Queue: " << std::endl;
  for (auto iter = this->key_access_queue.begin(); iter != this->key_access_queue.end(); iter++)
  {
    auto access_count = this->key_access_count[iter->key];
    std::cout << "  " << std::setw(20) << iter->key;
    if (access_count)
      std::cout << ", " << std::setw(10) << access_count << ", " << std::setw(10) << 
      std::chrono::duration_cast<std::chrono::milliseconds>(iter->timestamp.time_since_epoch()).count();
    std::cout << std::endl;
  }

  std::cout << std::endl;
}

}