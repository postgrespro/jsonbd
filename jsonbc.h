#ifndef JSONBC_H
#define JSONBC_H

#include <postgres.h>
#include <semaphore.h>

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
	sem_t			workers_sem;
	volatile int	workers_ready;
} jsonbc_shm_hdr;

typedef struct jsonbc_shm_worker
{
	shm_mq			   *mqin;
	shm_mq			   *mqout;
	pg_atomic_flag		busy;
	PGPROC			   *proc;
} jsonbc_shm_worker;

typedef struct jsonbc_pair
{
	int32	 id;
	char	*key;
} jsonbc_pair;

typedef struct jsonbc_cached_cmopt
{
	Oid		 cmoptoid;
	HTAB	*key_cache;
	HTAB	*id_cache;
} jsonbc_cached_opt;

typedef struct jsonbc_cached_key
{
	uint32	 keyhash;
	uint32	 pairslen;
	jsonbc_pair	**pairs;	/* for hash collisions */
} jsonbc_cached_key;

typedef struct jsonbc_cached_id
{
	uint32	 id;
	jsonbc_pair	*pair;
} jsonbc_cached_id;

extern void _PG_init(void);
extern void jsonbc_register_worker(int n);

extern void *workers_data;
extern int jsonbc_nworkers;
extern int jsonbc_cache_size;
extern int jsonbc_queue_size;

#endif
