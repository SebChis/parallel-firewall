/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include <pthread.h>

#include "ring_buffer.h"
#include "packet.h"

typedef struct log_entry_t {
	unsigned long timestamp;
	unsigned long seq;
	char log_line[256];
	struct log_entry_t *next;
} log_entry_t;

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;

    /* TODO: add synchronization primitives for timestamp ordering */
	const char *out_filename;
	pthread_mutex_t *log_mutex;

	log_entry_t **log_queue;
	pthread_mutex_t *queue_mutex;
	pthread_cond_t *queue_cond;
	int *finished_consumers;
	int num_consumers;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename,
					pthread_mutex_t *log_mutex,
					log_entry_t **log_queue,
					pthread_mutex_t *queue_mutex,
					pthread_cond_t *queue_cond,
					int *finished_consumers);

void *writer_thread(void *arg);

#endif /* __SO_CONSUMER_H__ */
