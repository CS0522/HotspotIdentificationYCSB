#include "twitter_trace_reader.h"

namespace module
{
bool TwitterTraceReader::CheckThreadId(const size_t thread_id)
{
  bool valid = (thread_id < thread_count_);
  if (!valid)
    YCSB_C_LOG_ERROR("thread_id: %zu is out of range", thread_id);
  
  return valid;
}

TwitterTraceReader::TwitterTraceReader()
{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader initialized");
}

TwitterTraceReader::TwitterTraceReader(const std::string& trace_file_path)
{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader initialized");
  if (this->ReadTraceFile(trace_file_path))
    YCSB_C_LOG_INFO("Read trace file: %s completed", trace_file_path.c_str());
  else
    std::runtime_error("Read trace file failed\n");
}

TwitterTraceReader::TwitterTraceReader(const std::string& trace_file_path, 
                                       const size_t thread_count)
  : thread_count_(thread_count), thread_local_index_(thread_count)
{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader with multi-thread: %zu", thread_count_);
  if (this->ReadTraceFile(trace_file_path))
    YCSB_C_LOG_INFO("Read trace file: %s completed", trace_file_path.c_str());
  else
    std::runtime_error("Read trace file failed\n");

  // 初始化多线程索引计数
  for (auto& index : thread_local_index_)
    index.store(0, std::memory_order_relaxed);
}

TwitterTraceReader::TwitterTraceReader(const std::string& trace_file_path, const size_t thread_count, 
                                       const size_t limit_record_count, const size_t limit_operation_count)
  : thread_count_(thread_count), thread_local_index_(thread_count),
    limit_record_count_(limit_record_count), limit_operation_count_(limit_operation_count)

{
  YCSB_C_LOG_INFO("Twitter Cache-trace reader with multi-thread: %zu", thread_count_);

  if (this->ReadTraceFile(trace_file_path))
    YCSB_C_LOG_INFO("Read trace file: %s completed", trace_file_path.c_str());
  else
    std::runtime_error("Read trace file failed\n");

  // 初始化多线程索引计数
  for (auto& index : thread_local_index_)
    index.store(0, std::memory_order_relaxed);
}

bool TwitterTraceReader::ReadTraceFile(const std::string& trace_file_path, const char delimiter)
{
  std::ifstream trace_file(trace_file_path);
  if (!trace_file.is_open())
  {
    YCSB_C_LOG_ERROR("Error opening trace file: %s", trace_file_path.c_str());
    return false;
  }

  std::string line;
  size_t line_cnt = 0;
  bool no_error = true;
  size_t max_read_line_cnt = 0;
  // 节约内存资源，只读取指定范围的 trace
  if ((this->limit_record_count_ || this->limit_operation_count_))
    max_read_line_cnt = std::max(this->limit_record_count_, this->limit_operation_count_);
  while (std::getline(trace_file, line))
  {
    if (line_cnt >= max_read_line_cnt)
      break;
    
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
      YCSB_C_LOG_ERROR("Invalid line format at %zu: %s", line_cnt, line.c_str());
      no_error = false;
      continue;
    }
    try
    {
      Request req;
      // timestamp field
      // req.timestamp = static_cast<uint64_t>(std::stoul(fields[0]));
      // anonymized_key field
      req.anonymized_key = fields[1];
      // key_size field
      req.key_size = static_cast<uint32_t>(std::stoul(fields[2]));
      // value_size field
      req.value_size = static_cast<uint32_t>(std::stoul(fields[3]));
      // client_id field
      // req.client_id = static_cast<uint32_t>(std::stoul(fields[4]));
      // operation field (default: SET)
      auto op_iter = g_op_map.find(fields[5]);
      req.operation = (op_iter != g_op_map.end()) 
          ? op_iter->second 
          : TwitterTraceOperation::SET;
      // ttl field
      // req.ttl = static_cast<uint32_t>(std::stoul(fields[6]));
      this->trace_requests_.push_back(req);
    }
    catch(const std::exception& e)
    {
      YCSB_C_LOG_ERROR("Parse line error: %s, at %zu: %s", e.what(), line_cnt, line.c_str());
      no_error = false;
    }
    
  }

  YCSB_C_LOG_INFO("Read completed, total line count: %zu", line_cnt);
  // set iterator
  this->trace_iter_ = this->trace_requests_.begin();
  this->read_succeeded_ = no_error;
  return no_error;
}

bool TwitterTraceReader::GetTraceRequests(std::vector<Request>& requests)
{
  requests = this->trace_requests_;
  return true;
}

size_t TwitterTraceReader::GetAllRequestsCount()
{
  return this->trace_requests_.size();
}

Request* TwitterTraceReader::JumpToFirst()
{
  for (auto& index : this->thread_local_index_) 
    index.store(0, std::memory_order_relaxed);

  this->trace_iter_ = this->trace_requests_.begin();
  this->curr_request_ptr_ = &(*(this->trace_iter_));
  return curr_request_ptr_;
}

Request* TwitterTraceReader::JumpToLast()
{
  this->trace_iter_ = this->trace_requests_.end();
  this->trace_iter_--;
  this->curr_request_ptr_ = &(*(this->trace_iter_));
  return curr_request_ptr_;
}

Request* TwitterTraceReader::GetNext()
{
  if (this->trace_requests_.empty())
    return nullptr;
  
  if (this->trace_iter_ == this->trace_requests_.end())
    this->trace_iter_ = this->trace_requests_.begin();
  
  this->curr_request_ptr_ = &(*(this->trace_iter_));
  // remove to the next position
  if (std::next(this->trace_iter_) != this->trace_requests_.end())
    this->trace_iter_++;
  
  return curr_request_ptr_;
}

Request* TwitterTraceReader::GetNextByThread(size_t thread_id)
{
  if (!CheckThreadId(thread_id))
    exit(EXIT_FAILURE);

  // local index
  size_t current_index = (thread_local_index_[thread_id].fetch_add(1, std::memory_order_relaxed));
  size_t target_request_index = current_index * thread_count_ + thread_id;
  // check if is valid
  if (target_request_index >= trace_requests_.size())
    // maybe this thread's work is done
    return nullptr;

  return &trace_requests_[target_request_index];
}

std::string TwitterTraceReader::GetNextKey()
{
  std::string next_key = this->GetNext()->anonymized_key;
  return next_key;
}

std::string TwitterTraceReader::GetNextKeyByThread(size_t thread_id)
{
  auto next_req_ptr = this->GetNextByThread(thread_id);
  std::string next_key;
  if (next_req_ptr)
    next_key = next_req_ptr->anonymized_key;
  return next_key;
}

TwitterTraceOperation TwitterTraceReader::GetOperation()
{
  if (this->trace_iter_ == this->trace_requests_.end())
    this->trace_iter_ = this->trace_requests_.begin();
  
  Request* req = &(*(this->trace_iter_));
  TwitterTraceOperation op = req->operation;
  return op;
}

TwitterTraceOperation TwitterTraceReader::GetOperationByThread(size_t thread_id)
{
  if (!CheckThreadId(thread_id))
    exit(EXIT_FAILURE);

  // local index
  size_t current_index = (thread_local_index_[thread_id].load(std::memory_order_relaxed));
  size_t target_request_index = current_index * thread_count_ + thread_id;

  TwitterTraceOperation op = TwitterTraceOperation::GET;
  if (target_request_index < trace_requests_.size())
    op = trace_requests_[target_request_index].operation;
  return op;
  // 这里缺少处理越界场景
}

Request* TwitterTraceReader::GetCurrent()
{
  return this->curr_request_ptr_;
}

Request* TwitterTraceReader::GetCurrentByThread(size_t thread_id)
{
  if (!CheckThreadId(thread_id))
    exit(EXIT_FAILURE);

  // local index
  size_t current_index = (thread_local_index_[thread_id].load(std::memory_order_relaxed));
  size_t target_request_index = (current_index - 1) * thread_count_ + thread_id;

  // check if is valid
  if (target_request_index >= trace_requests_.size())
    // maybe this thread's work is done
    return nullptr;

  return &trace_requests_[target_request_index];
}

size_t TwitterTraceReader::GetCurrentValueSize()
{
  return ((this->curr_request_ptr_) ? this->curr_request_ptr_->value_size : 0);
}

size_t TwitterTraceReader::GetCurrentValueSizeByThread(size_t thread_id)
{
  auto current_req_ptr = GetCurrentByThread(thread_id);
  return ((current_req_ptr) ? current_req_ptr->value_size: 0);
}

size_t TwitterTraceReader::GetCurrentKeySize()
{
  return ((this->curr_request_ptr_) ? this->curr_request_ptr_->key_size : 0);
}

size_t TwitterTraceReader::GetCurrentKeySizeByThread(size_t thread_id)
{
  auto current_req_ptr = GetCurrentByThread(thread_id);
  return ((current_req_ptr) ? current_req_ptr->key_size : 0);
}

std::string TwitterTraceReader::GetCurrentKey()
{
  return ((this->curr_request_ptr_) ? this->curr_request_ptr_->anonymized_key : "");
}

std::string TwitterTraceReader::GetCurrentKeyByThread(size_t thread_id)
{
  auto current_req_ptr = GetCurrentByThread(thread_id);
  return ((current_req_ptr) ? current_req_ptr->anonymized_key : "");
}

Request* TwitterTraceReader::GetPrev()
{
  if (this->trace_requests_.empty())
    return nullptr;
  
  if (this->trace_iter_ == this->trace_requests_.end()) 
    this->trace_iter_ = std::prev(this->trace_requests_.end());
  
  this->curr_request_ptr_ = &(*(this->trace_iter_));
  // remove to the previous position
  if (this->trace_iter_ != this->trace_requests_.begin())
    this->trace_iter_--;
  
  return this->curr_request_ptr_;
}

Request* TwitterTraceReader::GetPrevByThread(size_t thread_id)
{
  if (!CheckThreadId(thread_id))
    exit(EXIT_FAILURE);

  // local index
  size_t current_index = (thread_local_index_[thread_id].fetch_sub(1, std::memory_order_relaxed));
  size_t target_request_index = current_index * thread_count_ + thread_id;
  // check if is valid
  if (target_request_index >= trace_requests_.size() || target_request_index < 0)
    // maybe this thread's work is done
    return nullptr;

  return &trace_requests_[target_request_index];
}

std::string TwitterTraceReader::GetPrevKey()
{
  std::string prev_key = this->GetPrev()->anonymized_key;
  return prev_key;
}

std::string TwitterTraceReader::GetPrevKeyByThread(size_t thread_id)
{
  auto prev_req_ptr = this->GetNextByThread(thread_id);
  std::string prev_key;
  if (prev_req_ptr)
    prev_key = prev_req_ptr->anonymized_key;
  return prev_key;
}

void TwitterTraceReader::ResetIterator()
{
  // Run 阶段重放 Trace，iterator 重新归位
  this->trace_iter_ = this->trace_requests_.begin();

  for (auto& index : this->thread_local_index_) 
    index.store(0, std::memory_order_relaxed);
}

void TwitterTraceReader::TraverseTrace()
{
  if (!this->read_succeeded_)
    return;

  this->JumpToFirst();
  while (this->trace_iter_ != this->trace_requests_.end())
  {
    // std::cout << this->trace_iter_->timestamp << ", " << this->trace_iter_->anonymized_key
    //           << ", " << this->trace_iter_->key_size << ", " << this->trace_iter_->value_size
    //           << ", " << this->trace_iter_->client_id << ", " << this->trace_iter_->operation
    //           << ", " << this->trace_iter_->ttl << std::endl;

    std::cout << this->trace_iter_->anonymized_key
              << ", " << this->trace_iter_->key_size << ", " << this->trace_iter_->value_size
              << ", " << this->trace_iter_->operation
              << std::endl;

    this->trace_iter_++;
  }
  this->JumpToFirst();
}

inline void DisplayRequest(const Request& req)
{
  // std::cout << req.timestamp << ", " << req.anonymized_key
  //           << ", " << req.key_size << ", " << req.value_size
  //           << ", " << req.client_id << ", " << req.operation
  //           << ", " << req.ttl << std::endl;
  std::cout << req.anonymized_key
            << ", " << req.key_size << ", " << req.value_size
            << ", " << req.operation
            << std::endl;
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
//   YCSB_C_LOG_INFO("Jump to first");
//   tmp = trace_reader.JumpToFirst();
//   for (size_t i = 0; i < cnt; i++)
//   {
//     tmp = trace_reader.GetNext();
//     if (tmp)
//       module::DisplayRequest(*tmp);
//   }

//   return 0;  
// }