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

