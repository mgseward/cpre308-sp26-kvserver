Concurrent Clients      Total Operations        Throughput
1                            10000            60530.16 ops/sec
4                            40000            301576.11 ops/sec
16                          160000            387890.44 ops/sec
64                          640000            396254.43 ops/sec

The throughput is linear between 1 and 4 concurrent clients but when it reachest 16 it begins to plateau seeing very little change from 16 to 64 comparatively (around 10000 ops/sec difference from 16 t0 64). 
This is occuring because there are only 4 worker threads being used, so the threads are always busy and the additional request coming through for them are forced to join the wait queue until the next worker becomes free.
Although the worker threads are being overworked, the use of the lock at the bucket level is avoiding the lock contention during the write workload.
