# cpre308-sp26-kvserver
Project 2 for CPRE308
By: Mark Seward

## 1. Build and Run Instructions
1. **Compile the code:** `make`
2. **Start the server:** `./kvserver <port> <num_workers> <num_buckets> [sweeper_interval_ms]`
   * `port`: TCP port to listen on.
   * `num_workers`: Number of concurrent worker threads.
   * `num_buckets`: Size of the hash table.
   * `sweeper_interval_ms`: (Optional) Time between TTL checks.

## 2. Stage Status
* **Stage 1 (Sequential Accept):** Completed.
* **Stage 2 (Thread Pool & Queue):** Completed. Implemented a bounded work queue using condition variables.
* **Stage 3 (RW Locking):** Completed.
* **Stage 4 (TTL Sweeper & Stats):** Completed.
* **Extra Credit:** Completed. Implemented Fine-Grained (Bucket-Level) Locking for optimized concurrency.
* **Known Bugs/Limitations:** There are no bugs which I confirmed with the zero memory leaks reported by Valgrind.

## 3. Design Decisions Made:

1) I chose the bucket level locking for this project. The downside is that it increases memory overhead and requires a more complex initialization process because I needed an array of pthread_rwlock_t objects, one for each bucket. Although there is much better performance benefits, since I used a global lock so the writer blocks all the other worker threads, no matter what key they hold. The use of the bucket level locking limits clashing to the threads hashing to the same bucket. for my stress tests I used 1024 buckets and 50 writers. The results were very quick response times because the threads waiting for the same bucket is very low since there are so many more buckets than writers. This is important because the server works sideways with the hash table which avoids a problem with threads waiting with heavy congestion.

2) For the worker pool size, I tested using all scales (2, 4, 8, 16) worker threads. I saw a linear performance result up to 4 workeres on the environment but at 8 workers the performance dropped of a lot with 16 workers having throughput begin to decrease. Because of heavy context switching and mutex lock conflicts, when the number of threads exceeded the available CPU cores, the OS was swapping threads across the CPU which was many cycles of time where no tasks able to be performed during this time. Also, all the worker threads need to grab the global_stats.lock to update the stats and the queue.lock to get new connections. Using 16 threads for this meant the time spent waiting in the queue for the locks was longer than the benefits of using more worker threads. The worker count matching the CPU cores was the ideal situation for optimization.

3) The sweeper thread is a background maintenance process, and this was a big design decision I made because it ensures how to handle locking in the sweep processing. If the sweeper had a global write lock for the whole pass, the server would freeze all the clients until the entire  table was swept through meaning every single, 1024 buckets. To counteract this I made the sweeper only lock for a single bucket at a time. The sweeper would enter the loop, have the write lock for the bucket the loop was in, clean all the expired nodes in the linked list link, and then release the lock before going to the next bucket in the look. This was an important decision I made because the worker threads were allowed to continue accessing all the other burckets in the table while the sweeper accessed its own bucket causing no disturbances to others. This situation was more difficult to design, but it helps limit long waiting times for the sweeper to look at all the buckets and makes sure that there is lots of availability.
