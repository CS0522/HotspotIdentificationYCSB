#include "twitter_trace_reader.h"

namespace module
{
TwitterTraceReader::TwitterTraceReader(bool enable_mmap)
  : enable_mmap_(enable_mmap), mmap_reader_ptr_(nullptr)
{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader, enable mmap: %d\n", enable_mmap_);
}

TwitterTraceReader::TwitterTraceReader(bool enable_mmap, const std::string& trace_file_path)
  : enable_mmap_(enable_mmap), mmap_reader_ptr_(nullptr)
{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader, enable mmap: %d\n", enable_mmap_);
  if (!this->enable_mmap_)
  {
    if (this->ReadTraceFile(trace_file_path))
      YCSB_C_LOG_INFO("Read trace file: %s completed\n", trace_file_path.c_str());
    else
      std::runtime_error("Read trace file failed\n");
  }
  else
  {
    if (this->ReadTraceFileByMmap(trace_file_path))
      YCSB_C_LOG_INFO("Read (by mmap) trace file: %s completed\n", trace_file_path.c_str());
    else
      std::runtime_error("Read trace file failed\n");
  }
}

bool TwitterTraceReader::ReadTraceFile(const std::string& trace_file_path, const char delimiter)
{
  std::ifstream trace_file(trace_file_path);
  if (!trace_file.is_open())
  {
    YCSB_C_LOG_ERROR("Error opening trace file: %s\n", trace_file_path.c_str());
    return false;
  }

  std::string line;
  size_t line_cnt = 0;
  bool no_error = true;
  while (std::getline(trace_file, line))
  {
    line_cnt++;
    if (line.empty())
      continue;
    // split by delimiter
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, delimiter))
      fields.push_back(field);
    // parse the fields string
    if (fields.size() < 7)
    {
      YCSB_C_LOG_ERROR("Invalid line format at %zu: %s\n", line_cnt, line.c_str());
      no_error = false;
      continue;
    }
    try
    {
      Request req;
      // timestamp field
      req.timestamp = static_cast<uint64_t>(std::stoul(fields[0]));
      // anonymized_key field
      req.anonymized_key = fields[1];
      // key_size field
      req.key_size = static_cast<uint32_t>(std::stoul(fields[2]));
      // value_size field
      req.value_size = static_cast<uint32_t>(std::stoul(fields[3]));
      // client_id field
      req.client_id = static_cast<uint32_t>(std::stoul(fields[4]));
      // operation field (default: SET)
      auto op_iter = g_op_map.find(fields[5]);
      req.operation = (op_iter != g_op_map.end()) 
          ? op_iter->second 
          : TwitterTraceOperation::SET;
      // ttl field
      req.ttl = static_cast<uint32_t>(std::stoul(fields[6]));
      this->trace_requests_.push_back(req);
      this->key_value_size_map_[req.anonymized_key] = std::max(req.value_size, this->key_value_size_map_[req.anonymized_key]);
    }
    catch(const std::exception& e)
    {
      YCSB_C_LOG_ERROR("Parse line error: %s, at %zu: %s\n", e.what(), line_cnt, line.c_str());
      no_error = false;
    }
    
  }

  YCSB_C_LOG_INFO("Read completed, total line count: %zu\n", line_cnt);
  // set iterator
  this->trace_iter_ = this->trace_requests_.begin();
  this->map_iter_ = this->key_value_size_map_.begin();
  this->read_succeeded_ = no_error;
  return no_error;
}

bool TwitterTraceReader::GetTraceRequests(std::vector<Request>& requests)
{
  if (!this->enable_mmap_)
    requests = this->trace_requests_;
  return true;
}

size_t TwitterTraceReader::GetAllRequestsCount()
{
  if (!this->enable_mmap_)
    return this->trace_requests_.size();
}

bool TwitterTraceReader::GetAllKeys(std::vector<std::string>& keys)
{
  if (!this->enable_mmap_)
  {
    for (auto map_iter = this->key_value_size_map_.begin(); map_iter != key_value_size_map_.end(); map_iter++)
      keys.push_back(map_iter->first);
  }
  return true;
}

size_t TwitterTraceReader::GetAllKeysCount()
{
  return this->key_value_size_map_.size();
}

bool TwitterTraceReader::GetAllKeysValueSizeMap(std::unordered_map<std::string, uint32_t>& key_value_size_map)
{
  if (!this->enable_mmap_)
  {
    key_value_size_map = this->key_value_size_map_;
  }
  return true;
}

size_t TwitterTraceReader::GetMaxValueSizeOfKey(const std::string& key)
{
  auto map_iter = this->key_value_size_map_.find(key);
  if (map_iter != this->key_value_size_map_.end())
    return map_iter->second;
  else
  {
    YCSB_C_LOG_ERROR("Error: cannot find value size of key: %s\n", key.c_str());
    return 0;
  }
}

std::string TwitterTraceReader::GetNextSequenceKey()
{
  std::string key;
  if (this->map_iter_ != this->key_value_size_map_.end())
  {
    key = this->map_iter_->first;
    this->map_iter_++;
  }

  return key;
}

bool TwitterTraceReader::ReadTraceFileByMmap(const std::string& trace_file_path)
{
  if (!this->enable_mmap_)
  {
    YCSB_C_LOG_ERROR("Invoked ReadTraceFileByMmap(), but 'enable_mmap' is false. Invoking ReadTraceFile() instead...\n");
    return this->ReadTraceFile(trace_file_path);
  }
  
  return false;
}

Request* TwitterTraceReader::JumpToFirst()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_ = this->trace_requests_.begin();
    return &(*(this->trace_iter_));
  }
  
  return nullptr;
}
Request* TwitterTraceReader::JumpToLast()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_ = this->trace_requests_.end();
    this->trace_iter_--;
    return &(*(this->trace_iter_));
  }

  return nullptr;
}
Request* TwitterTraceReader::GetNext()
{
  if (this->trace_requests_.empty())
    return nullptr;
  
  if (!this->enable_mmap_)
  {
    if (this->trace_iter_ == this->trace_requests_.end())
      this->trace_iter_ = this->trace_requests_.begin();
    
    Request* curr_ptr = &(*(this->trace_iter_));
    // if is at the last element position
    if (std::next(this->trace_iter_) == this->trace_requests_.end())
      return nullptr;
    // else remove to the next position
    else
      this->trace_iter_++;
    
    return curr_ptr;
  }

  return nullptr;
}
Request* TwitterTraceReader::GetPrev()
{
  if (this->trace_requests_.empty())
    return nullptr;
  
  if (!this->enable_mmap_)
  {
    if (this->trace_iter_ == this->trace_requests_.end()) 
      this->trace_iter_ = std::prev(this->trace_requests_.end());
    
    Request* curr_ptr = &(*(this->trace_iter_));
    // if is at the first element position
    if (this->trace_iter_ == this->trace_requests_.begin())
      return nullptr;
    // else remove to the previous position
    else
      this->trace_iter_--;
    
    return curr_ptr;
  }

  return nullptr;
}

void TwitterTraceReader::TraverseTrace()
{
  if (!this->read_succeeded_)
    return;

  if (!this->enable_mmap_)
  {
    while (this->trace_iter_ != this->trace_requests_.end())
    {
      std::cout << this->trace_iter_->timestamp << ", " << this->trace_iter_->anonymized_key
                << ", " << this->trace_iter_->key_size << ", " << this->trace_iter_->value_size
                << ", " << this->trace_iter_->client_id << ", " << this->trace_iter_->operation
                << ", " << this->trace_iter_->ttl << std::endl;
  
      this->trace_iter_++;
    }
  }
}

inline void DisplayRequest(const Request& req)
{
  std::cout << req.timestamp << ", " << req.anonymized_key
            << ", " << req.key_size << ", " << req.value_size
            << ", " << req.client_id << ", " << req.operation
            << ", " << req.ttl << std::endl;
}

}

// int main()
// {
//   module::TwitterTraceReader trace_reader(false, "/home/frank/workspace/test/cluster024.txt");
//   // trace_reader.TraverseTrace();

//   // >>>>>>>>>> TEST <<<<<<<<<<
//   size_t cnt = 10;
//   module::Request* tmp;
//   tmp = trace_reader.JumpToFirst();
//   if (tmp)
//     module::DisplayRequest(*tmp);
//   tmp = trace_reader.JumpToLast();
//   if (tmp)
//     module::DisplayRequest(*tmp);
//   for (size_t i = 0; i < cnt; i++)
//   {
//     tmp = trace_reader.GetPrev();
//     if (tmp)
//       module::DisplayRequest(*tmp);
//   }
//   YCSB_C_LOG_INFO("Jump to first" << std::endl;
//   tmp = trace_reader.JumpToFirst();
//   for (size_t i = 0; i < cnt; i++)
//   {
//     tmp = trace_reader.GetNext();
//     if (tmp)
//       module::DisplayRequest(*tmp);
//   }

//   return 0;  
// }