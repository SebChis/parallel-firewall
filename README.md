# Parallel Firewall

This repository implements a parallel firewall simulation in C using `pthread` and a thread-safe ring buffer. The program reads fake packet data from an input file, processes it in parallel with multiple consumers, and writes ordered log output.

## Project Overview

### `firewall.c`

- Program entry point
- Initializes a shared ring buffer and synchronization primitives
- Creates consumer threads and a single writer thread
- Waits for all threads to finish and cleans up

### `ring_buffer.c`

- Implements a fixed-size circular buffer
- Supports thread-safe enqueue/dequeue using `pthread_mutex_t` and `pthread_cond_t`
- Handles buffer stop and cleanup
- Keeps packet sequence order via an `ordin` index array

### `producer.c`

- Reads input packets from a file
- Enqueues packets into the ring buffer
- Signals end-of-data to consumers

### `consumer.c`

- Each consumer thread dequeues packets and processes them
- Computes `PASS`/`DROP` decisions and packet hashes
- Inserts processed log entries into an ordered in-memory queue by sequence number
- Writer thread flushes the queue to output in exact packet order

## Why This Design

- Producer/consumer parallelization with multiple consumers
- No busy waiting: consumers wait on condition variables
- Output log is written in ascending packet order without post-processing sort
- A dedicated writer thread ensures ordered file output even when consumers finish out of order

## What Is Implemented

- `ring_buffer_init()`
- `ring_buffer_enqueue()`
- `ring_buffer_dequeue()`
- `ring_buffer_stop()`
- `ring_buffer_destroy()`
- Consumer thread creation and synchronization
- Ordered logging using a linked queue and sequence numbers
- Writer thread that writes logs only when the next sequence is ready

## Build

```bash
make
```

This creates the `firewall` binary.

## Run

```bash
./firewall <input-file> <output-file> <num-consumers>
```

## Notes

- `num-consumers` must be between **1 and 32**
- Threads synchronize using `pthread_mutex_t` and `pthread_cond_t`
- Log entries are written in order as processing proceeds
- The ring buffer stops cleanly once the producer finishes reading input

## File Structure

```text

├── firewall.c          # Main orchestration
├── ring_buffer.c       # Thread-safe circular buffer implementation
├── ring_buffer.h
├── producer.c          # Packet publishing
├── producer.h
├── consumer.c          # Consumer threads and ordered logging
├── consumer.h
└── packet.h            # Packet format and processing helpers
```
