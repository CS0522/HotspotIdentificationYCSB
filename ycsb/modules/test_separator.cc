#include "separator.h"
#include "heat_separator_lru_k.h"

#include <random>
#include <chrono>

using namespace module;

void usage()
{
  std::cerr << "Separator Algorithm: " << std::endl;
  std::cerr << "  LRU-K: 0" << std::endl;
}

int main(int argc, char *argv[])
{
  if (argc < 2)
  {
    std::cerr << "Missing Argument" << std::endl;
    usage();
    exit(EXIT_FAILURE);
  }
  int algorithm_arg = std::atoi(argv[1]); 
  
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> dis(1, 50);

  HeatSeparator *heat_separator = nullptr;
  if (algorithm_arg == 0)
    heat_separator = new HeatSeparatorLruK(3, 10);

  if (!heat_separator)
  {
    std::cerr << "Null Ptr of Heat Separator" << std::endl;
    exit(EXIT_FAILURE);
  }

  for (int i = 0; i < 200; i++)
  {
    auto num = dis(gen);
    int res = heat_separator->Put(std::to_string(num));
    std::cout << "Put " << (res == SUCCESS ? "succeeded" : "failed") << ": " << num << std::endl;
    if (i % 10 == 0)
    {
      auto get_num = i >= 10 ? (i - 10) : 0;
      res = heat_separator->Get(std::to_string(get_num));
      std::cout << "Get " << (res == SUCCESS ? "succeeded" : "failed") << ": " << get_num << std::endl;
    }
    // if (i % 40 == 0)
    heat_separator->Display();
  }

  return 0;
}