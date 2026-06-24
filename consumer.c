// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../utils/utils.h"

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"

typedef struct {
	so_packet_t pkt;
	unsigned long seq;
} pkt_with_seq_t;

void *consumer_thread(void *arg)
{
	/* TODO: implement consumer thread */
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)arg;
	pkt_with_seq_t data;
	ssize_t result;
	char out_buf[256];
	int len;
	log_entry_t *new_entry;

	while (1) {
		result = ring_buffer_dequeue(ctx->producer_rb, &data, sizeof(data));

		if (result <= 0)
			break;

		so_action_t action = process_packet(&data.pkt);
		unsigned long hash = packet_hash(&data.pkt);
		unsigned long timestamp = data.pkt.hdr.timestamp;

		len = snprintf(out_buf, sizeof(out_buf), "%s %016lx %lu\n",
					RES_TO_STR(action), hash, timestamp);

		new_entry = malloc(sizeof(log_entry_t));
		DIE(new_entry == NULL, "malloc");

		new_entry->timestamp = timestamp;
		new_entry->seq = data.seq;
		memcpy(new_entry->log_line, out_buf, len + 1);
		new_entry->next = NULL;

		pthread_mutex_lock(ctx->queue_mutex);
		log_entry_t **link = ctx->log_queue;

		while (*link != NULL && (*link)->seq < data.seq)
			link = &((*link)->next);

		new_entry->next = *link;
		*link = new_entry;

		pthread_cond_broadcast(ctx->queue_cond);
		pthread_mutex_unlock(ctx->queue_mutex);
	}

	pthread_mutex_lock(ctx->queue_mutex);
	(*ctx->finished_consumers)++;
	pthread_cond_broadcast(ctx->queue_cond);
	pthread_mutex_unlock(ctx->queue_mutex);

	free(ctx);
	return NULL;
}

void *writer_thread(void *arg)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)arg;
	int out_fd;
	log_entry_t *current;
	unsigned long expected_seq = 0;

	out_fd = open(ctx->out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
	DIE(out_fd < 0, "open");

	pthread_mutex_lock(ctx->queue_mutex);

	while (1) {
		if (*ctx->log_queue != NULL && (*ctx->log_queue)->seq == expected_seq) {
			current = *ctx->log_queue;
			*ctx->log_queue = current->next;

			pthread_mutex_unlock(ctx->queue_mutex);
			pthread_mutex_lock(ctx->log_mutex);
			write(out_fd, current->log_line, strlen(current->log_line));
			pthread_mutex_unlock(ctx->log_mutex);

			free(current);
			expected_seq++;

			pthread_mutex_lock(ctx->queue_mutex);
			continue;
		}

		if (*ctx->finished_consumers == ctx->num_consumers && *ctx->log_queue == NULL)
			break;

		pthread_cond_wait(ctx->queue_cond, ctx->queue_mutex);
	}

	pthread_mutex_unlock(ctx->queue_mutex);
	close(out_fd);
	free(ctx);
	return NULL;
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename,
					 pthread_mutex_t *log_mutex,
					 log_entry_t **log_queue,
					 pthread_mutex_t *queue_mutex,
					 pthread_cond_t *queue_cond,
					 int *finished_consumers)
{
	*finished_consumers = 0;

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

		DIE(ctx == NULL, "malloc");

		ctx->producer_rb = rb;
		ctx->out_filename = out_filename;
		ctx->log_mutex = log_mutex;
		ctx->log_queue = log_queue;
		ctx->queue_mutex = queue_mutex;
		ctx->queue_cond = queue_cond;
		ctx->finished_consumers = finished_consumers;
		ctx->num_consumers = num_consumers;

		int rc = pthread_create(&tids[i], NULL, consumer_thread, ctx);

		DIE(rc != 0, "pthread_create");
	}

	return num_consumers;
}
