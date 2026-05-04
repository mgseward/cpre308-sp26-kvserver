/*
 * kvserver.c -- Mini-KV server entry point
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * Starter scaffolding: this file gives you a working TCP listener and an
 * argument parser. Everything else -- accept loop, protocol, hash table,
 * thread pool, RW locking, TTL sweeper -- is yours to write.
 *
 * Build: run `make` in this directory. See the provided Makefile.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#include "kv.h"

/* -------- Globals ------------------------------------------------------- */

static volatile sig_atomic_t g_shutdown = 0;

static void sigint_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

/* -------- Socket helpers ------------------------------------------------ */

/* Create a listening TCP socket bound to the given port. Returns fd or -1. */
static int make_listen_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, 64) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* -------- Entry point --------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <port> <num_workers> <num_buckets> [sweeper_interval_ms]\n"
        "   port                TCP port to listen on (1-65535)\n"
        "   num_workers         number of worker threads (>=1)\n"
        "   num_buckets         hash-table bucket count (>=1)\n"
        "   sweeper_interval_ms default 500\n",
        prog);
}

/* Stage 1: Sequential Accept Loop:
*/


	/*Creation of the hash table and linked list memory storage
	*/
typedef struct node
{
	char key[257];
	char value[257];
	time_t expires_at;
	struct node *next;
}	
	node_t;
	
typedef struct hashTable
{
	node_t **groups;
	int numGroups;
	pthread_rwlock_t *rwlocks;
}
	table_t;
	
table_t *global_table;

typedef struct{
	int hits;
	int misses;
	int puts;
	int deletes;
	int keys;
	int activeConnections;
	time_t start;
	pthread_mutex_t lock;
}
	stats_t;
	
stats_t global_stats;
	
table_t* init_table(int numGroups)
{
	table_t *table = malloc(sizeof(table_t));
	table->numGroups = numGroups;
	table->groups = calloc(numGroups, sizeof(node_t*));
	table->rwlocks = malloc(numGroups * sizeof(pthread_rwlock_t));
	for(int f = 0; f < numGroups; f++)
	{
		pthread_rwlock_init(&table->rwlocks[f], NULL);
	}
	return table;
}

typedef struct
{
	int *fds;
	int capacity;
	int head;
	int tail;
	int count;
	pthread_mutex_t lock;
	pthread_cond_t not_empty;
	pthread_cond_t not_full;
}
	workQueue;
	
	workQueue queue;
	pthread_t *workers;

	
	
unsigned int hashing(const char *key, int numGroups)
{
		unsigned int hashNum = 5381;
		int a;
		while ((a = *key++))
		{
			hashNum = ((hashNum << 5) + hashNum) + a;
		}
		return hashNum % numGroups;
}
	/** Cleanup process for the memory. Loops trhough each group andwalks down the linked list. Every group thrown away has the pointer save to the next box. Itr then unties the groups, frees it and moves on. Once all boxes are gone then the free function can be used on the group and eventually the structure of the table as well.
	
	*/
void freeTable(table_t *table)
{
	if(!table)
	{
		return;
	}
	for(int b = 0; b < table->numGroups; b++)
	{
		node_t *current = table->groups[b];
		while(current != NULL)
		{
			node_t *nextNode = current->next;
			free(current);
			current = nextNode;
		}
		pthread_rwlock_destroy(&table->rwlocks[b]);
	}
	free(table->rwlocks);
	free(table->groups);
	free(table);
}

void initializeQue(int numWorkers)
{
	queue.capacity = numWorkers * 2;
	queue.fds =  malloc(sizeof(int) * queue.capacity);
	queue.head = 0;
	queue.tail = 0;
	queue.count = 0;
	pthread_mutex_init(&queue.lock, NULL);
	pthread_cond_init(&queue.not_empty, NULL);
	pthread_cond_init(&queue.not_full, NULL);
}

/** The producer function. Locks mutex, checks if queue is full, then calls condition if it is. Places the connection to the tail. The array is designed as a circle and unlocks the mutex when not_empty is true and now wants to accept connections
*/
void enque(int conn_fd)
{
	pthread_mutex_lock(&queue.lock);
	
	//Checks to see if the queue is at max capacity and if it is all threads sleep until queue is not longer full
	while(queue.count == queue.capacity)
	{
		pthread_cond_wait(&queue.not_full, &queue.lock);
	}
	
	queue.fds[queue.tail] = conn_fd;
	queue.tail = (queue.tail + 1) % queue.capacity;
	queue.count++;
	
	pthread_cond_signal(&queue.not_empty);
	pthread_mutex_unlock(&queue.lock);
}

/** Ran by the worker threads, lock the mutex. If the array is empty they sleep waiting for it to be full. The g_shutdown check is there for the Ctrl+C kill to empty the queue and will return -1 to time out and die. If not then they grab the file descriptor at the head, wrap the head using the math and ring when not_full and unlock the mutex
*/
int deque()
{
	pthread_mutex_lock(&queue.lock);
	
	while(queue.count == 0 && !g_shutdown)
	{
		pthread_cond_wait(&queue.not_empty, &queue.lock);
	}
	if(g_shutdown && queue.count == 0)
	{
		pthread_mutex_unlock(&queue.lock);
		return -1;
	}
	
	int conn_fd = queue.fds[queue.head];
	queue.head = (queue.head + 1) % queue.capacity;
	queue.count--;
	
	pthread_cond_signal(&queue.not_full);
	pthread_mutex_unlock(&queue.lock);
	
	return conn_fd;
}

void put_key(const char *key, const char *val, int ttl) 
{
	unsigned int idx = hashing(key, global_table->numGroups);
	pthread_rwlock_wrlock(&global_table->rwlocks[idx]);
	
    node_t *current = global_table->groups[idx];

    while (current != NULL) 
    {
        if (strcmp(current->key, key) == 0) 
        {
            strcpy(current->value, val);
            current->expires_at = (ttl > 0) ? time(NULL) + ttl : 0;
            pthread_rwlock_unlock(&global_table->rwlocks[idx]);
            return;
        }
        current = current->next;
    }

    node_t *newNode = malloc(sizeof(node_t));
    strcpy(newNode->key, key);
    strcpy(newNode->value, val);
    newNode->expires_at = (ttl > 0) ? time(NULL) + ttl : 0;
    newNode->next = global_table->groups[idx];
    global_table->groups[idx] = newNode;
    
    pthread_mutex_lock(&global_stats.lock);
    global_stats.keys++;
    pthread_mutex_unlock(&global_stats.lock);
    pthread_rwlock_unlock(&global_table->rwlocks[idx]);
}

int get_key(const char *key, char *val_out) 
{
	    unsigned int idx = hashing(key, global_table->numGroups);
	pthread_rwlock_rdlock(&global_table->rwlocks[idx]);
    node_t *current = global_table->groups[idx];

    while (current != NULL) 
    {
        if (strcmp(current->key, key) == 0) 
        {
            strcpy(val_out, current->value);
            pthread_rwlock_unlock(&global_table->rwlocks[idx]);
            return 1;
        }
        current = current->next;
    }
    pthread_rwlock_unlock(&global_table->rwlocks[idx]);
    return 0;
}

int del_key(const char *key) 
{
	    unsigned int idx = hashing(key, global_table->numGroups);
	pthread_rwlock_wrlock(&global_table->rwlocks[idx]);
    node_t *current = global_table->groups[idx];
    node_t *prev = NULL;

    while (current != NULL) 
    {
        if (strcmp(current->key, key) == 0) 
        {
        
            if (prev == NULL) 
            {
                global_table->groups[idx] = current->next;
            } 
            else 
            {
                prev->next = current->next;
            }
            free(current);
                pthread_mutex_lock(&global_stats.lock);
    		global_stats.keys--;
    		pthread_mutex_unlock(&global_stats.lock);
            pthread_rwlock_unlock(&global_table->rwlocks[idx]);
            return 1;
        }
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&global_table->rwlocks[idx]);
    return 0;
}
     
void *sweeper_loop(void *arg)
{

	int interval_ms = *(int *)arg;
	while(!g_shutdown)
	{
		usleep(interval_ms * 1000);
		if(g_shutdown)break;
		
		time_t now = time(NULL);
		
		for(int e = 0; e < global_table->numGroups; e++)
		{
			pthread_rwlock_wrlock(&global_table->rwlocks[e]);
			node_t *curr = global_table->groups[e];
			node_t *prev = NULL;
	
			while(curr != NULL)
			{
				if(curr->expires_at > 0 && curr->expires_at <= now)
				{
					node_t *temp = curr;
					if(prev == NULL)
					{
						global_table->groups[e] = curr->next;
					}
					else
					{	
						prev->next = curr->next;
					}
					curr = curr->next;
					free(temp);
			
					pthread_mutex_lock(&global_stats.lock);
					global_stats.keys--;
					pthread_mutex_unlock(&global_stats.lock);
				}
			else
			{
				prev = curr;
				curr = curr->next;
			}
	}
	pthread_rwlock_unlock(&global_table->rwlocks[e]);
	}
	}
	return NULL;
}

     /**Command code statements
     */
void commandHandler(int conn_fd)
{
	pthread_mutex_lock(&global_stats.lock);
	global_stats.activeConnections++;
	pthread_mutex_unlock(&global_stats.lock);
	
     FILE *stream = fdopen(conn_fd, "r");
     if(!stream)
     {
     	close(conn_fd);
     	return;
     }
     char *sentence = NULL;
     size_t length = 0;
     ssize_t read;
     
     //getLine pauses proram waiting for a client message. Reads bytes until a \n and asks OS for RAM neded, stores the sentence in line and keeps looping until connection closes.
     while((read = getline(&sentence, &length, stream)) != -1)
     {
     //Making fixed memory in stack to hold individual words sent over
     	char cmd[16] = {0};
     	char key[257] = {0};
     	char val[257] = {0};
     	int textLine = 0;
     	//divides the sentence. cmd is 15 char limit (first word), key is second word (limit 256) val is 3rd. $th word turns to an intereger and spits parsed variable to see how man of these things it found.
     	int parsed = sscanf(sentence, "%15s %256s %256s %d", cmd, key, val, &textLine);
     	
     	if(parsed > 0)
     	{
     	
     		if(strcmp(cmd, "QUIT") == 0)
     		{
     			dprintf(conn_fd, "BYE\n");
     			break;
     		}
     		else if(strcmp(cmd, "GET") == 0 && parsed >= 2)
     		{
     			char foundValue[257];
     			if(get_key(key, foundValue))
     			{
     				pthread_mutex_lock(&global_stats.lock);
				global_stats.hits++;
				pthread_mutex_unlock(&global_stats.lock);
     				dprintf(conn_fd, "VALUE %s\n", foundValue);
     			}
     			else
     			{
     				pthread_mutex_lock(&global_stats.lock);
				global_stats.misses++;
				pthread_mutex_unlock(&global_stats.lock);
				dprintf(conn_fd, "NOT found\n");
			}
		}
		else if(strcmp(cmd, "PUT") == 0 && parsed >= 3)
     		{
     			put_key(key, val, textLine);
     			pthread_mutex_lock(&global_stats.lock);
			global_stats.puts++;
			pthread_mutex_unlock(&global_stats.lock);
			dprintf(conn_fd, "OK\n");
		}
		else if(strcmp(cmd, "DEL") == 0 && parsed >= 2)
     		{
     			if(del_key(key))
     			{
     				pthread_mutex_lock(&global_stats.lock);
				global_stats.deletes++;
				pthread_mutex_unlock(&global_stats.lock);
     				dprintf(conn_fd, "OK\n");
     			}
     			else
     			{
				dprintf(conn_fd, "NOT FOUND\n");
			}
		}
		else if(strcmp(cmd, "STATS") == 0)
     		{
     			time_t uptime = time(NULL) - global_stats.start;
     			pthread_mutex_lock(&global_stats.lock);
			dprintf(conn_fd, "STATS keys=%d hits=%d misses=%d puts=%d deletes=%d activeConnections=%d uptime_s=%ld\n", global_stats.keys, global_stats.hits, global_stats.misses, global_stats.puts, global_stats.deletes, global_stats.activeConnections, uptime);
			pthread_mutex_unlock(&global_stats.lock);
		}
		else
		{
			dprintf(conn_fd, "ERROR for bad command prompt\n");
		}
	}
	}
     	free(sentence);
     	fclose(stream);
     	
     	pthread_mutex_lock(&global_stats.lock);
	global_stats.activeConnections--;
	pthread_mutex_unlock(&global_stats.lock);
}

/**	Infinite loop that all worker threads run. Constantly runs deque, waits for a connection and passes it toe the commandHandler, then closes the socket.
*/
void *workerLoop(void *arg)
{
	(void)arg;
	while(1)
	{
		int conn= deque();
		if (conn < 0)
		{
			break;
		}
		commandHandler(conn);
		close(conn);
	}
	return NULL;
}

int main(int argc, char **argv) {
    if (argc < 4 || argc > 5) {
        usage(argv[0]);
        return 1;
    }
    int port         = atoi(argv[1]);
    int num_workers  = atoi(argv[2]);
    int num_buckets  = atoi(argv[3]);
    int sweeper_ms   = (argc == 5) ? atoi(argv[4]) : 500;

    if (port <= 0 || port > 65535 || num_workers < 1 ||
        num_buckets < 1 || sweeper_ms <= 0) {
        usage(argv[0]);
        return 1;
    }

    /* Install Ctrl-C handler for clean shutdown. */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ignore SIGPIPE: writes to closed sockets should fail with EPIPE, not
     * kill the server. */
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = make_listen_socket(port);
    if (listen_fd < 0) return 1;

    fprintf(stderr,
        "kvserver: listening on port %d "
        "(workers=%d, buckets=%d, sweeper=%dms)\n",
        port, num_workers, num_buckets, sweeper_ms);

    /* ================================================================
     * TODO (Stage 1): Sequential accept loop.
     *   while (!g_shutdown) {
     *       int conn = accept(listen_fd, NULL, NULL);
     *       if (conn < 0) { ...handle EINTR on signal, else perror... }
     *       handle_client(conn);
     *       close(conn);
     *   }
     *
     * TODO (Stage 2): Initialize work queue + spawn worker threads.
     *                 The accept loop now enqueues conn fds instead of
     *                 calling handle_client directly.
     *
     * TODO (Stage 3): Initialize the hash table's rwlock before the accept
     *                 loop starts.
     *
     * TODO (Stage 4): Spawn the sweeper thread; join it on shutdown.
     *
     * TODO (shutdown): drain queue, join all threads, free everything.
     * ================================================================ */
     
     memset(&global_stats, 0, sizeof(global_stats));
     global_stats.start= time(NULL);
     pthread_mutex_init(&global_stats.lock, NULL);
     
     global_table = init_table(num_buckets);
     
     /**while (!g_shutdown) 
     {
     	int conn = accept(listen_fd, NULL, NULL);
     	if (conn < 0) 
     	{
     		if(errno == EINTR)
     		{
			continue;
		}
		break;
	}
	commandHandler(conn);
	close(conn);
     }
     freeTable(global_table);
     close(listen_fd);
     return 0;
     */
     
     
     initializeQue(num_workers);
     workers = malloc(sizeof(pthread_t) * num_workers);
     for(int c = 0; c < num_workers; c++)
     {
     	pthread_create(&workers[c], NULL, workerLoop, NULL);
     }
     pthread_t sweeper_tid;
     pthread_create(&sweeper_tid, NULL, sweeper_loop, &sweeper_ms);
     
     while(!g_shutdown)
     {
	int conn = accept(listen_fd, NULL, NULL);
	if(conn < 0)
	{
		if(errno == EINTR)
		{
			continue;
		}
		perror("accept");
		break;
	}
	enque(conn);
	}
	
	pthread_mutex_lock(&queue.lock);
	pthread_cond_broadcast(&queue.not_empty);
	pthread_mutex_unlock(&queue.lock);
	
	for(int d = 0; d < num_workers; d++)
	{
		pthread_join(workers[d], NULL);
	}
	pthread_join(sweeper_tid, NULL);
	pthread_mutex_destroy(&global_stats.lock);
	
	free(workers);
	free(queue.fds);
	freeTable(global_table);
	close(listen_fd);
	return 0;
}
