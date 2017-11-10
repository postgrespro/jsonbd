#ifndef JSONBC_H
#define JSONBC_H

#include <postgres.h>

#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"

#define JSONBC_SHM_MQ_MAGIC		0xAAAA
#define MAX_JSONBC_WORKERS		10

typedef enum {
	JSONBC_CMD_GET_IDS,
	JSONBC_CMD_GET_KEYS
} JsonbcCommand;

typedef struct jsonbc_shm_hdr
{
	volatile int workers_ready;
} jsonbc_shm_hdr;

typedef struct jsonbc_shm_worker
{
	shm_mq			   *mqin;
	shm_mq			   *mqout;
	pg_atomic_flag		busy;
	PGPROC			   *proc;
} jsonbc_shm_worker;

extern void _PG_init(void);
extern void jsonbc_register_worker(int n);

extern void *workers_data;
extern int jsonbc_nworkers;
extern int jsonbc_cache_size;
extern int jsonbc_queue_size;

extern void shm_mq_clean_receiver(shm_mq *mq);
extern void shm_mq_clean_sender(shm_mq *mq);

#endif
