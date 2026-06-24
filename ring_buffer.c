// SPDX-License-Identifier: BSD-3-Clause

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "ring_buffer.h"
#include "packet.h"
#include "../utils/utils.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->data = malloc(cap);
	if (ring->data == NULL)
		return -1;

	ring->ordin = malloc((cap / PKT_SZ) * sizeof(unsigned long));
	if (ring->ordin == NULL) {
		free(ring->data);
		return -1;
	}

	ring->write_pos = 0;
	ring->read_pos = 0;
	ring->cap = cap;
	ring->len = 0;
	ring->stopped = 0;
	ring->producer_idx = 0;

	if (pthread_mutex_init(&ring->mutex, NULL) != 0) {
		free(ring->data);
		free(ring->ordin);
		return -1;
	}

	if (pthread_cond_init(&ring->not_empty, NULL) != 0) {
		pthread_mutex_destroy(&ring->mutex);
		free(ring->data);
		free(ring->ordin);
		return -1;
	}

	if (pthread_cond_init(&ring->not_full, NULL) != 0) {
		pthread_mutex_destroy(&ring->mutex);
		pthread_cond_destroy(&ring->not_empty);
		free(ring->data);
		free(ring->ordin);
		return -1;
	}

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	if (size != PKT_SZ)
		return -1;

	pthread_mutex_lock(&ring->mutex);

	while (ring->len == ring->cap)
		pthread_cond_wait(&ring->not_full, &ring->mutex);

	memcpy(ring->data + ring->write_pos, data, size);

	size_t idx = ring->write_pos / PKT_SZ;

	ring->ordin[idx] = ring->producer_idx++;
	ring->write_pos = (ring->write_pos + size) % ring->cap;
	ring->len += size;

	pthread_cond_signal(&ring->not_empty);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	pthread_mutex_lock(&ring->mutex);

	while (ring->len == 0 && !ring->stopped)
		pthread_cond_wait(&ring->not_empty, &ring->mutex);

	if (ring->stopped && ring->len == 0) {
		pthread_mutex_unlock(&ring->mutex);
		return 0;
	}

	memcpy(data, ring->data + ring->read_pos, PKT_SZ);

	if (size >= PKT_SZ + sizeof(unsigned long)) {
		size_t idx = ring->read_pos / PKT_SZ;
		unsigned long *seq_ptr = (unsigned long *)((char *)data + PKT_SZ);
		*seq_ptr = ring->ordin[idx];
	}

	ring->read_pos = (ring->read_pos + PKT_SZ) % ring->cap;
	ring->len -= PKT_SZ;

	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return PKT_SZ;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	free(ring->data);
	free(ring->ordin);
	pthread_mutex_destroy(&ring->mutex);
	pthread_cond_destroy(&ring->not_empty);
	pthread_cond_destroy(&ring->not_full);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	pthread_mutex_lock(&ring->mutex);
	ring->stopped = 1;
	pthread_cond_broadcast(&ring->not_empty);
	pthread_cond_broadcast(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);
}
