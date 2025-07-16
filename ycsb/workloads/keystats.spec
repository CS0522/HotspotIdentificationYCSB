# Yahoo! Cloud System Benchmark
# Workload keystats 
#   统计 Key 的 Zipfian 分布，画出 pdf                
#   Read/Update ratio: 0/100
#   Default data size: 4 KiB records (1 fields, 4096 bytes each (dynamic), plus key)
#   Request distribution: zipfian

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
fieldcount=1

# dynamic
fieldlength=4096

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0
readmodifywriteproportion=0

# 修改
recordcount=10000
operationcount=10000

requestdistribution=zipfian
