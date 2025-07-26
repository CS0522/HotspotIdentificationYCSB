// Created by Frank Chen.
#ifndef YCSB_C_TWITTER_TRACE_GENERATOR_H_
#define YCSB_C_TWITTER_TRACE_GENERATOR_H_

#include "generator.h"

#include <cassert>
#include <cmath>
#include <cstdint>
#include <mutex>
#include "utils.h"

namespace ycsbc {

class TwitterTraceGenerator : public Generator<uint64_t>
{
 public:
  TwitterTraceGenerator();
  uint64_t Next(uint64_t num_items);
  uint64_t Next();
  uint64_t Last();
  
 private:
  
};



}

#endif // YCSB_C_TWITTER_TRACE_GENERATOR_H_
