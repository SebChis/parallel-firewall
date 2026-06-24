// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "ring_buffer.h"
#include "consumer.h"
#include "producer.h"
#include "log/log.h"
#include "packet.h"
#include "../utils/utils.h"

#define SO_RING_SZ		(PKT_SZ * 1000)

pthread_mutex_t MUTEX_LOG;
pthread_mutex_t QUEUE_MUTEX;
pthread_cond_t QUEUE_COND;

log_entry_t *LOG_QUEUE;
int FINISHED_CONSUMERS;

void log_lock(bool lock, void *udata)
{
	pthread_mutex_t *LOCK = (pthread_mutex_t *) udata;

	if (lock)
		pthread_mutex_lock(LOCK);
	else
		pthread_mutex_unlock(LOCK);
}

void __attribute__((constructor)) init()
{
	pthread_mutex_init(&MUTEX_LOG, NULL);
	pthread_mutex_init(&QUEUE_MUTEX, NULL);
	pthread_cond_init(&QUEUE_COND, NULL);
	log_set_lock(log_lock, &MUTEX_LOG);
}

void __attribute__((destructor)) dest()
{
	pthread_mutex_destroy(&MUTEX_LOG);
	pthread_mutex_destroy(&QUEUE_MUTEX);
	pthread_cond_destroy(&QUEUE_COND);
}

int main(int argc, char **argv)
{
	so_ring_buffer_t ring_buffer;
	int num_consumers, rc;
	pthread_t *thread_ids = NULL;
	pthread_t writer_tid;
	so_consumer_ctx_t *writer_ctx;

	if (argc < 4) {
		fprintf(stderr, "Usage %s <input-file> <output-file> <num-consumers:1-32>\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	rc = ring_buffer_init(&ring_buffer, SO_RING_SZ);
	DIE(rc != 0, "ring_buffer_init");

	num_consumers = strtol(argv[3], NULL, 10);

	if (num_consumers <= 0 || num_consumers > 32) {
		fprintf(stderr, "num-consumers [%d] must be in the interval [1-32]\n", num_consumers);
		exit(EXIT_FAILURE);
	}

	thread_ids = calloc(num_consumers, sizeof(pthread_t));
	DIE(thread_ids == NULL, "calloc pthread_t");

	writer_ctx = malloc(sizeof(so_consumer_ctx_t));
	DIE(writer_ctx == NULL, "malloc");
	writer_ctx->out_filename = argv[2];
	writer_ctx->log_mutex = &MUTEX_LOG;
	writer_ctx->log_queue = &LOG_QUEUE;
	writer_ctx->queue_mutex = &QUEUE_MUTEX;
	writer_ctx->queue_cond = &QUEUE_COND;
	writer_ctx->finished_consumers = &FINISHED_CONSUMERS;
	writer_ctx->num_consumers = num_consumers;

	DIE(pthread_create(&writer_tid, NULL, writer_thread, writer_ctx) != 0,
		"pthread_create writer");

	/* create consumer threads */
	create_consumers(thread_ids, num_consumers, &ring_buffer,
								argv[2], &MUTEX_LOG,
								&LOG_QUEUE, &QUEUE_MUTEX, &QUEUE_COND,
								&FINISHED_CONSUMERS);

	/* start publishing data */
	publish_data(&ring_buffer, argv[1]);

	/* TODO: wait for child threads to finish execution*/
	for (int i = 0; i < num_consumers; i++)
		pthread_join(thread_ids[i], NULL);

	pthread_mutex_lock(&QUEUE_MUTEX);
	pthread_cond_broadcast(&QUEUE_COND);
	pthread_mutex_unlock(&QUEUE_MUTEX);

	pthread_join(writer_tid, NULL);

	ring_buffer_destroy(&ring_buffer);

	free(thread_ids);

	return 0;
}

