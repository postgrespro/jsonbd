#ifndef JSONBD_H
#define JSONBD_H

#include <postgres.h>
#include <semaphore.h>

#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"

#define JSONBD_SHM_MQ_MAGIC		0xAAAA

#define JSONBD_LWLOCKS_TRANCHE	"jsonbd lwlocks tranche"
#define MAX_JSONBD_WORKERS_PER_DATABASE		3
#define MAX_DATABASES						10 /* FIXME: need more? */
#define MAX_JSONBD_WORKERS	(MAX_DATABASES * MAX_JSONBD_WORKERS_PER_DATABASE)

typedef enum {
	JSONBD_CMD_GET_IDS,
	JSONBD_CMD_GET_KEYS
} JsonbcCommand;

typedef struct jsonbd_shm_worker
{
	shm_mq			   *mqin;
	shm_mq			   *mqout;
	PGPROC			   *proc;
	volatile Oid		dboid;	/* database of the worker */
	LWLock			   *lock;
	Latch				latch;
} jsonbd_shm_worker;

/* Shared memory structures */
typedef struct jsonbd_shm_hdr
{
	volatile int		workers_ready;
	jsonbd_shm_worker	launcher;
	Latch				launcher_latch;
} jsonbd_shm_hdr;

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

/* Worker launch arguments */
typedef struct jsonbd_worker_args
{
	int		worker_num;
	Oid		dboid;
	int		database_num;
} jsonbd_worker_args;

extern void _PG_init(void);
extern void jsonbd_register_launcher(void);
extern Oid jsonbd_get_dictionary_relid(void);

extern void *workers_data;
extern int jsonbd_nworkers;
extern int jsonbd_cache_size;
extern int jsonbd_queue_size;

#endif
