/*
 * bench_client.c -- Mini-KV concurrent benchmark client
 *
 * Project 2, CprE 3080, Spring 2026
 *
 * YOU write this file. The scaffolding here is intentionally minimal:
 * an argument parser and nothing else. Your job is to fill in the rest.
 *
 * Usage:
 *   ./bench_client <host> <port> <num_clients> <ops_per_client> <read_pct>
 *
 * Requirements (from the spec):
 *   - Spawn <num_clients> threads.
 *   - Each thread opens its own TCP connection to <host>:<port>.
 *   - Each thread issues <ops_per_client> operations.
 *   - <read_pct> percent of ops are GETs; the rest are PUTs.
 *   - Keys drawn from a small pool (~1000 keys) so GETs actually hit.
 *   - Report total wall-clock time and overall throughput (ops/sec).
 *
 * Hints:
 *   - Use clock_gettime(CLOCK_MONOTONIC, ...) to measure elapsed time.
 *   - Each thread needs its own rand_r() seed to avoid serialization on
 *     the global rand() lock.
 *   - Read the server's reply line-by-line; don't assume one TCP packet
 *     per command.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <time.h>

typedef struct{
	const char *host;
	int port;
	int ops_per_client;
	int read_pct;
	int thread;
}
thread_args_t;

static void usage(const char *prog) {
    fprintf(stderr,
        "usage: %s <host> <port> <num_clients> <ops_per_client> <read_pct>\n",
        prog);
}

void *run_client(void *arg)
{
	thread_args_t *args = (thread_args_t *)arg;
	unsigned int seed = (unsigned int)(time(NULL) ^ (args->thread << 16));
	
	int socketPort = socket(AF_INET, SOCK_STREAM, 0);
	if(socketPort < 0)
	{
		perror("socket");
		return NULL;
	}
	
	struct hostent *server = gethostbyname(args->host);
	if(server == NULL)
	{
		fprintf(stderr, "error: no such host %s\n", args->host);
		close(socketPort);
		return NULL;
	}
	
	struct sockaddr_in serv_addr;
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
	serv_addr.sin_port = htons(args->port);
	
	if(connect(socketPort, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	{
		perror("connect");
		close(socketPort);
		return NULL;
	}
	
	FILE *stream = fdopen(socketPort, "r+");
	if(!stream)
	{
		close(socketPort);
		return NULL;
	}
	char *line = NULL;
	size_t len = 0;
	for(int a = 0; a < args->ops_per_client; a++)
	{
		int key = rand_r(&seed) % 1000;
		if((rand_r(&seed) % 100) < args->read_pct)
		{
			fprintf(stream, "GET key%d\n", key);
		}
		else
		{
			fprintf(stream, "PUT key%d val%d 0\n", key, a);
		}
		fflush(stream);
		
		if(getline(&line, &len, stream) == -1)
		{
			break;
		}
		
	}
	
	fprintf(stream, "QUIT\n");
	fflush(stream);
	if(getline(&line, &len, stream) != -1)
	{
	/**
	*/
	}
	
	free(line);
	fclose(stream);
	return NULL;
}
	
	

int main(int argc, char **argv) {
    if (argc != 6) {
        usage(argv[0]);
        return 1;
    }

    const char *host      = argv[1];
    int port              = atoi(argv[2]);
    int num_clients       = atoi(argv[3]);
    int ops_per_client    = atoi(argv[4]);
    int read_pct          = atoi(argv[5]);

    if (port <= 0 || num_clients < 1 || ops_per_client < 1 ||
        read_pct < 0 || read_pct > 100) {
        usage(argv[0]);
        return 1;
    }

    //(void)host;  /* silence warnings until you implement */

    /* TODO:
     *   1. Spawn num_clients pthreads.
     *   2. Each thread: connect to <host>:<port>, run ops_per_client
     *      operations mixing GETs and PUTs per read_pct.
     *   3. Join all threads.
     *   4. Compute and print total elapsed time and total ops/sec.
     */
     
     pthread_t *threads = malloc(sizeof(pthread_t) * num_clients);
     thread_args_t *t_args = malloc(sizeof(thread_args_t) * num_clients);
     
     struct timespec start;
     struct timespec end;
     clock_gettime(CLOCK_MONOTONIC, &start);
     
     for(int b = 0; b < num_clients; b++)
     {
     	t_args[b].host = host;
     	t_args[b].port = port;
     	t_args[b].ops_per_client = ops_per_client;
     	t_args[b].read_pct = read_pct;
     	t_args[b].thread = b;
     	pthread_create(&threads[b], NULL, run_client, &t_args[b]);
     	
     }
     for(int c = 0; c < num_clients; c++)
     {
     	pthread_join(threads[c], NULL);
     }
     clock_gettime(CLOCK_MONOTONIC, &end);
     
     double elapsed = (end.tv_sec - start.tv_sec)+(end.tv_nsec - start.tv_nsec) / 1000000000.0;
     long total_ops = (long)num_clients * ops_per_client;
     
     printf("Benchmark results:\n");
     printf("Total elapsed time: 	%.5f secpmds\n", elapsed);
     printf("Total operations 	%ld\n", total_ops);
     printf("Throuhgput:	%.2f ops/sec\n", total_ops/elapsed);
     
     free(threads);
     free(t_args);
     return 0;
  	

   // fprintf(stderr, "bench_client: not implemented yet\n");
    //return 0;
}
