# cpre308-sp26-kvserver
Project 2 for CPRE308
By: Mark Seward

Build and run Instructions:
1) make
2) ./kvswerver <port? <num_workers> <num_buckets> [sweeper_interval_ms]
   --> the port is the TCP
   --> num_workers is the # of concurrernt worker threads
   -->num_buckets = size of the hash table
   --> the sweeper_interval_ms is the time between the TTL checks

Completion of Stages:
Stage 1 --> Complete
Stage 2 --> Complete and implemented a bounded work queue using the condition variables
Stage 3 --> Complete
Stage 4 --> Complete
Extra Credit --> Implemented using Fine-Grained Locking for optimized concurrency.
Known Bugs --> None, the server is running as expected with no errors or leaks anywhere

Design Decisions Made:

1) I chose the bucket_level locking for this project. Although it increases overhead memory and has a more complex initiialization process because an array of pthread_rwlock_t objects is equivalent to the number of buckets. Although, there is much better performance benefits, since I used a global lock so the writer blocks all the other workier threads no matter what key they hold. The use of the buck-level locking limits contention to the threads hashing to the next buck and for my stress tests I used 1024 buckets and 50 writer. The results were very quick response times because the threads waiting for the same bucket is very low because there are so many more buckets than writers. This is important because the server scales horizontally with the hash table size and could be a potential choke point for a parallel system.

2) For the worker pool size, I tested using all scales (2, 4, 8, 16) worker threads. I saw a linear performance result up to 4 workeres on the environment but at 8 workers the performance dropped of a lot with 16 workers having throughput begin to decrease. Since there ias sheavy context sqirtching and mutex fighting, when the number of threads was larger than the available CPU cores, the OS was swapping threads through the CPU and was taking many cycles to do so with no tasks being able to be performed in this time frame. Also, all the worker threads need to grab the global_stats.lock to update the stats and the queue.lcok to fetch new connections. Using 16 threads for this meant the time spent waiting in queue for the locks was longer than the benefits of using more worker ethreads. The worker count matching the CPU cores was the ideal situation for optimization.

3) The sweeper thread is a background maintenacne process and this wasa a big design deicsion I made because it ensures how to handle locking in the sweep processing. If the sweeper had a global write lock for the whole pass, the server would freeze all the clients until the entire has table was swept through. To counteract this I made the sweeper only lock for a single bucket at a time. The sweeper would enter the loop, have the write lock for the bucket the loop was in, clean all the expired nodes in the linked list link, and then release the lock before going to the next bucket in the look. This was a key decision I made because the worker threads were allowed to continue accessing all the other burckets in the table while the sweeper worked in the single link in the list. This situation was more difficult to design, but it helps limit latency spikes waiting for the sweeper to look at all the buckets and makes sure that the background maintenance is not seen by the end user amking the availiability high even trhoughout lots of cleaning up done in the buclkets.
