#ifndef JSONBC_H
#define JSONBC_H

#include <postgres.h>

#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"

#define JSONBC_SHM_MQ_MAGIC		0xAAAA
#define MAX_JSONBC_WORKERS		10

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

extern void jsonbc_get_key_ids(Oid cmoptoid, char *buf, int buflen, uint32 *idsbuf, int nkeys);
extern void jsonbc_get_key_ids_slow(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys);

extern char *jsonbc_get_key_by_id(Oid cmoptoid, int32 key_id);
extern char *jsonbc_get_key_by_id_slow(Oid cmoptoid, int32 key_id);

extern void *workers_data;
extern int jsonbc_nworkers;
extern int jsonbc_cache_size;
extern int jsonbc_queue_size;

#endif
