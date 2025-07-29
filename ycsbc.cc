//
//  ycsbc.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/19/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include <memory>
#include <thread>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "core/histogram.h"
#include "db/db_factory.h"
#include "db/keystats_db.h"
#include "core/twitter_trace_workload.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);

static bool g_workload_twitter = false;

size_t DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const size_t num_ops,
    bool is_loading, shared_ptr<Histogram> hist) {
  db->Init();
  ycsbc::Client client(*db, wl);
  size_t oks = 0;
  utils::Timer timer;
  for (size_t i = 0; i < num_ops; ++i) {
    timer.Reset();
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
    double duration = timer.GetDurationUs();
    hist->Add(duration);

    if (oks % 100000 == 0)
      std::cout << "oks: " << oks << std::endl;
  }
  // db->Close();
  return oks;
}

int main(const int argc, const char *argv[]) {
  utils::Properties props;
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }
  
  ycsbc::CoreWorkload *wl = nullptr;
  if (g_workload_twitter)
    wl = new ycsbc::TwitterTraceWorkload();
  else
    wl = new ycsbc::CoreWorkload();
  wl->Init(props);

  // print some infos
  if (g_workload_twitter)
  {
    std::cout << "recordcount: " << ((ycsbc::TwitterTraceWorkload*)wl)->GetRecordCount() << std::endl;
    std::cout << "operationcount: " << ((ycsbc::TwitterTraceWorkload*)wl)->GetOperationCount() << std::endl;
  }
  else
  {
    std::cout << "fieldcount: " << props.GetProperty(ycsbc::CoreWorkload::FIELD_COUNT_PROPERTY, ycsbc::CoreWorkload::FIELD_COUNT_DEFAULT) << std::endl;
    std::cout << "fieldlength: " << props.GetProperty(ycsbc::CoreWorkload::FIELD_LENGTH_PROPERTY, ycsbc::CoreWorkload::FIELD_LENGTH_DEFAULT) << std::endl;
    std::cout << "zero_padding: " << props.GetProperty(ycsbc::CoreWorkload::ZERO_PADDING_PROPERTY, ycsbc::CoreWorkload::ZERO_PADDING_DEFAULT) << std::endl;
    std::cout << "record_count: " << props.GetProperty(ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY) << std::endl;
    std::cout << "operation_count: " << props.GetProperty(ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY) << std::endl;
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  vector<shared_ptr<Histogram>> hists;
  // Loads data
  utils::Timer timer;
  timer.Reset();
  vector<future<size_t>> actual_ops;
  size_t total_ops;
  if (g_workload_twitter)
    total_ops = ((ycsbc::TwitterTraceWorkload*)wl)->GetRecordCount();
  else
    total_ops = std::stoul(props.GetProperty(ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY));
  for (int i = 0; i < num_threads; ++i) {
    auto hist = make_shared<Histogram>();
    hist->Clear();
    hists.emplace_back(hist);
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, wl, total_ops / num_threads, true, hist));
  }
  assert(actual_ops.size() == (size_t)num_threads);

  size_t sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  double duration = timer.GetDurationMs();
  // Send Special Command
  db->Special("PAUSE");
  for (int i = 1; i < num_threads; i++) {
    hists[0]->Merge(*hists[i]);
  }
  cout << "\n" << props["dbname"] << '\t' << file_name << '\t' << num_threads << '\n';
  cout << "# Load duration (sec): " << duration / 1000.0 << endl;
  cout << "# Loading records:\t" << sum << endl;
  cout << "# Load throughput (KOPS): ";
  cout << total_ops / duration << endl;
  cout << hists[0]->ToString() << endl;

  // // Load 与 Run 之间停 3 秒
  // std::cout << "Waiting 3s before performing transactions......" << std::endl;
  std::cout << "Skipped Load Stage, waiting 3s......" << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(3));
  std::cout << "Starting performing transactions......" << std::endl << std::endl;

  // Peforms transactions
  hists.clear();
  actual_ops.clear();
  if (g_workload_twitter)
  {
    total_ops = ((ycsbc::TwitterTraceWorkload*)wl)->GetOperationCount();
    // 使 Reader 迭代器归位
    ((ycsbc::TwitterTraceWorkload*)wl)->ResetIterator();
  }
  else
    total_ops = std::stoul(props.GetProperty(ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY));
  
  timer.Reset();
  for (int i = 0; i < num_threads; ++i) {
    auto hist = make_shared<Histogram>();
    hist->Clear();
    hists.emplace_back(hist);
    actual_ops.emplace_back(async(launch::async,
        DelegateClient, db, wl, total_ops / num_threads, false, hist));
  }
  assert(actual_ops.size() == (size_t)num_threads);

  sum = 0;
  for (auto &n : actual_ops) {
    assert(n.valid());
    sum += n.get();
  }
  duration = timer.GetDurationMs();
  // Send Special Command
  db->Special("STOP");
  for (int i = 1; i < num_threads; i++) {
    hists[0]->Merge(*hists[i]);
  }
  cout << "# Run duration (sec): " << duration / 1000.0 << endl;
  cout << "# Run operations:\t" << sum << endl;
  cout << "# Run throughput (KOPS): ";
  cout << total_ops / duration << endl;
  cout << hists[0]->ToString() << endl;
  
  // Key 统计功能、显式转换后输出到文件
  if (props["dbname"] == "key_stats")
  {
    auto keystats_db = static_cast<ycsbc::KeyStatsDB*>(db);
    keystats_db->OutputStats();
  }

  delete db;
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 1;
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    }
    // 添加 workload 参数
    else if (strcmp(argv[argindex], "-workload") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      std::string workload_type(argv[argindex]);
      if (workload_type == "twitter")
        g_workload_twitter = true;
      argindex++;
    }
    // 添加 fieldlength 参数
    else if (strcmp(argv[argindex], "-fieldlength") == 0)
    {
        argindex++;
        if (argindex >= argc)
        {
            UsageMessage(argv[0]);
            exit(0);
        }
        props.SetProperty("fieldlength", argv[argindex]);
        argindex++;
    }
    // 添加 fieldcount 参数
    else if (strcmp(argv[argindex], "-fieldcount") == 0)
    {
        argindex++;
        if (argindex >= argc)
        {
            UsageMessage(argv[0]);
            exit(0);
        }
        props.SetProperty("fieldcount", argv[argindex]);
        argindex++;
    }
    // 添加 recordcount 参数
    else if (strcmp(argv[argindex], "-recordcount") == 0)
    {
        argindex++;
        if (argindex >= argc)
        {
            UsageMessage(argv[0]);
            exit(0);
        }
        props.SetProperty("recordcount", argv[argindex]);
        argindex++;
    }
    // 添加 operationcount 参数
    else if (strcmp(argv[argindex], "-operationcount") == 0)
    {
        argindex++;
        if (argindex >= argc)
        {
            UsageMessage(argv[0]);
            exit(0);
        }
        props.SetProperty("operationcount", argv[argindex]);
        argindex++;
    }
    else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -workload workload type: 'core' or 'twitter'" << endl;
  cout << "  -server server: (Deprecated) specify the server address, e.g. 127.0.0.1:50051" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
  cout << "  Be sure that the two following args are after the \'-P\'" << endl;
  cout << "  -fieldlength: specify the fieldlength, cover the attribute in workload file" << endl;
  cout << "  -recordcount: specify the recordcount, cover the attribute in workload file" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

