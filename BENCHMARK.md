| Concurrent Clients | Total Operations | Throughput (ops/sec) |
| :--- | :--- | :--- |
| 1 | 10,000 | 60,530.16 |
| 4 | 40,000 | 301,576.11 |
| 16 | 160,000 | 387,890.44 |
| 64 | 640,000 | 396,254.43 |

The throughput is linear between 1 and 4 concurrent clients, but at 16 it begins to plateau, with comparatively little change from 16 to 64 (around 10000 ops/sec). 
This is occurring because there are only 4 worker threads being used, so the threads are always busy and the additional requests coming through for them are forced to join the wait queue until the next worker becomes free.
Although the worker threads are overworked, the bucket-level lock prevents lock contention during the write workload.
