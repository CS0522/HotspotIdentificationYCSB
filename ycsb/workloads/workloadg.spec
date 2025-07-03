# Yahoo! Cloud System Benchmark
# Workload G: Update only
#   Application example: user profile cache, where profiles are constructed elsewhere (e.g., Hadoop)
#                        
#   Read/update ratio: 0/100
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=100000
operationcount=100000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=zipfian



