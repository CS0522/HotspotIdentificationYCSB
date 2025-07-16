//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"

#include <string>
#include "db/basic_db.h"
#include "db/lock_stl_db.h"
#include "db/tbb_rand_db.h"
#include "db/tbb_scan_db.h"
// #include "db/rocksdb_db.h"
#include "db/keystats_db.h"

const std::string rocksdb_path{"/mnt/nvme1/rocksdb_data"};

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties &props) {
  if (props["dbname"] == "basic") {
    return new BasicDB;
  } else if (props["dbname"] == "lock_stl") {
    return new LockStlDB;
  } else if (props["dbname"] == "tbb_rand") {
    return new TbbRandDB;
  } else if (props["dbname"] == "tbb_scan") {
    return new TbbScanDB;
  } else if (props["dbname"] == "rocksdb") {
    std::filesystem::create_directories(rocksdb_path);
    return nullptr;
  }
  // 对 Key 分布统计时，使用 key_stats
  else if (props["dbname"] == "key_stats")
  {
    return new KeyStatsDB;
  } else {
    return NULL;
  }
}

