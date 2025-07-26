#include "twitter_trace_reader.h"

namespace module
{
TwitterTraceReader::TwitterTraceReader(bool enable_mmap)
  : enable_mmap_(enable_mmap), mmap_reader_ptr_(nullptr)
{
  std::cout << "Twitter Cache-trace reader, enable mmap: " << enable_mmap_ << std::endl;
}

TwitterTraceReader::TwitterTraceReader(bool enable_mmap, const std::string& trace_file_path)
  : enable_mmap_(enable_mmap), mmap_reader_ptr_(nullptr)
{
  std::cout << "Twitter Cache-trace reader, enable mmap: " << enable_mmap_ << std::endl;
  if (!this->enable_mmap_)
  {
    if (this->ReadTraceFile(trace_file_path))
      std::cout << "Read trace file: " << trace_file_path << " completed" << std::endl;
    else
      std::cout << "Read trace file failed" << std::endl;
  }
  else
  {
    if (this->ReadTraceFileByMmap(trace_file_path))
      std::cout << "Read (by mmap) trace file: " << trace_file_path << " completed" << std::endl;
    else
      std::cout << "Read (by mmap) trace file failed" << std::endl;
  }
}

bool TwitterTraceReader::ReadTraceFile(const std::string& trace_file_path, const char delimiter)
{
  std::ifstream trace_file(trace_file_path);
  if (!trace_file.is_open())
  {
    std::cerr << "Error opening trace file: " << trace_file_path << std::endl;
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
      std::cerr << "Invalid line format at " << line_cnt << ": " << line << std::endl;
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
          : OperationType::SET;
      // ttl field
      req.ttl = static_cast<uint32_t>(std::stoul(fields[6]));
      this->trace_requests_.push_back(req);
    }
    catch(const std::exception& e)
    {
      std::cerr << "Parse line error: " << e.what() 
                << ", at " << line_cnt << ": " << line << std::endl;
      no_error = false;
    }
    
  }

  std::cout << "Read completed, total line count: " << line_cnt << std::endl;
  // set iterator
  this->trace_iter_ = this->trace_requests_.begin();
  this->read_succeeded_ = no_error;
  return no_error;
}

std::vector<Request> TwitterTraceReader::GetTraceRequests()
{
  if (!this->enable_mmap_)
    return this->trace_requests_;
}

bool TwitterTraceReader::ReadTraceFileByMmap(const std::string& trace_file_path)
{
  if (!this->enable_mmap_)
  {
    std::cerr << "Invoked ReadTraceFileByMmap(), but 'enable_mmap' is false. Invoking ReadTraceFile() instead..." << std::endl;
    return this->ReadTraceFile(trace_file_path);
  }
  
}

Request TwitterTraceReader::GetFirst()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_ = this->trace_requests_.begin();
    return *(this->trace_iter_);
  }
}
Request TwitterTraceReader::GetLast()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_ = this->trace_requests_.end();
    this->trace_iter_--;
    return *(this->trace_iter_);
  }
}
Request TwitterTraceReader::GetNext()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_++;
    return *(this->trace_iter_);
  }
}
Request TwitterTraceReader::GetPrev()
{
  if (!this->enable_mmap_)
  {
    this->trace_iter_--;
    return *(this->trace_iter_);
  }
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

}

// int main()
// {
//   module::TwitterTraceReader trace_reader(false, "/home/frank/workspace/test/cluster024.txt");
//   trace_reader.TraverseTrace();

//   return 0;  
// }