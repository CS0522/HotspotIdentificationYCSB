# YCSB-C

Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)

## Quick Start

To build YCSB-C on Ubuntu, for example:

```
$ sudo apt-get install libtbb-dev
$ make
```

Run Workload A with a [TBB](https://www.threadingbuildingblocks.org)-based
implementation of the database, for example:

```
./ycsbc -db tbb_rand -threads 4 -P workloads/workloada.spec
```

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the recordcount property. Reference properties
files in the workloads dir.

## 新增

* `keystats` Key 数据统计信息模块，作为 `-db` 参数

* `modules/` 下新增热点命令识别算法模块

* 添加 Twitter Cache-trace 支持且多线程安全，但无法实现保序（Trace 文件内时间戳顺序），通过 `-DTWITTER_TRACE=ON` 启用