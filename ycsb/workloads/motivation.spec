# Yahoo! Cloud System Benchmark
# Workload motivation: Update only # 动机实验的工作负载
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/Update ratio: 0/100
#   Default data size: 4 KiB records (1 fields, 4096 bytes each (dynamic), plus key)
#   Request distribution: uniform

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true
fieldcount=1

# dynamic
fieldlength=4096

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

# dynamic
# total_db_size = 100 GiB = 100 * 1024 * 1024 * 1024 bytes, so
# record_count = 100 * 1024 * 1024 * 1024 / field_length
recordcount=10000
# operation_count = 1M
operationcount=10000

requestdistribution=uniform



