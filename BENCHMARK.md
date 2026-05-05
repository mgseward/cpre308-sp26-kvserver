Concurrent Clients      Total Operations        Throughput
1                            10000            60530.16 ops/sec
4                            40000            301576.11 ops/sec
16                          160000            387890.44 ops/sec
64                          640000            396254.43 ops/sec

The throughput is linear between 1 and 4 concurrent clients, but at 16 it begins to plateau, with comparatively little change from 16 to 64 (around 10000 ops/sec). 
This is occurring because there are only 4 worker threads being used, so the threads are always busy and the additional requests coming through for them are forced to join the wait queue until the next worker becomes free.
Although the worker threads are overworked, the use of the bucket-level lock avoids lock contention during the write workload.
