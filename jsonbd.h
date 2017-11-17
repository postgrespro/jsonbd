#ifndef JSONBD_H
#define JSONBD_H

#include <postgres.h>
#include <semaphore.h>

#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"

#define JSONBD_SHM_MQ_MAGIC		0xAAAA
#define MAX_JSONBD_WORKERS		10
#define MAX_DATABASES			10

typedef enum {
	JSONBD_CMD_GET_IDS,
	JSONBD_CMD_GET_KEYS
} JsonbcCommand;

/* Shared memory structures */
typedef struct jsonbd_shm_hdr
{
	sem_t			workers_sem[MAX_DATABASES];
	volatile int	workers_ready;
} jsonbd_shm_hdr;

typedef struct jsonbd_shm_worker
{
	Oid					dboid;	/* database of the worker */
	sem_t			   *dbsem;
	shm_mq			   *mqin;
	shm_mq			   *mqout;
	pg_atomic_flag		busy;
	PGPROC			   *proc;
} jsonbd_shm_worker;

/* CACHE */
typedef struct jsonbd_pair
{
	int32	 id;
	char	*key;
} jsonbd_pair;

typedef struct jsonbd_cached_cmopt
{
	Oid		 cmoptoid;
	HTAB	*key_cache;
	HTAB	*id_cache;
} jsonbd_cached_cmopt;

typedef struct jsonbd_cached_key
{
	uint32	 keyhash;
	List	*pairs;
} jsonbd_cached_key;

typedef struct jsonbd_cached_id
{
	uint32	 id;
	jsonbd_pair	*pair;
} jsonbd_cached_id;

/* Worker launch variables */
typedef struct jsonbd_worker_init
{
	char	dbname[NAMEDATALEN];
	int		shm_key;
} jsonc_worker_init;

extern void _PG_init(void);
extern void jsonbd_register_worker(int n);

extern void *workers_data;
extern int jsonbd_nworkers;
extern int jsonbd_cache_size;
extern int jsonbd_queue_size;

#endif