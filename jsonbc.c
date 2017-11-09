#include "jsonbc.h"

#include "postgres.h"
#include "fmgr.h"

#include "access/compression.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_crc.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(jsonbc_compression_handler);

/* we use one buffer for whole transaction to avoid extra allocations */
typedef struct
{
	char	   *buf;		/* keys */
	int			buflen;
	uint32	   *idsbuf;		/* key ids */
	int			idslen;

	MemoryContext	item_mcxt;
} CompressionThroughBuffers;

#if PG_VERSION_NUM == 110000
struct shm_mq_alt
{
	slock_t		mq_mutex;
	PGPROC	   *mq_receiver;	/* this one */
	PGPROC	   *mq_sender;		/* this one */
	uint64		mq_bytes_read;
	uint64		mq_bytes_written;
	Size		mq_ring_size;
	bool		mq_detached;	/* and this one */

	/* in postgres version there are more attributes, but we don't need them */
};
#else
#error "shm_mq struct in jsonbc is copied from PostgreSQL 11, please correct it according to your version"
#endif

/* local */
static MemoryContext compression_mcxt = NULL;
static CompressionThroughBuffers *compression_buffers = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shm_toc *toc = NULL;

/* global */
void *workers_data = NULL;
int jsonbc_nworkers = -1;
int jsonbc_cache_size = 0;
int jsonbc_queue_size = 0;

static void init_memory_context(bool);
static void memory_reset_callback(void *arg);
static void encode_varbyte(uint32 val, unsigned char *ptr, int *len);
static uint32 decode_varbyte(unsigned char *ptr);
static char *packJsonbValue(JsonbValue *val, int header_size, int *len);
static void setup_guc_variables(void);
static void shm_mq_clean_sender(shm_mq *mq);
static void shm_mq_clean_receiver(shm_mq *mq);
static void jsonbc_get_keys(Oid cmoptoid, uint32 *ids, int nkeys, char **keys);
static void jsonbc_get_key_ids(Oid cmoptoid, char *buf, int buflen, uint32 *idsbuf, int nkeys);

static inline Size
jsonbc_get_queue_size(void)
{
	return (Size) (jsonbc_queue_size * 1024);
}

static void
shm_mq_clean_sender(shm_mq *mq)
{
	struct shm_mq_alt	*amq = (struct shm_mq_alt *) mq;

	/* check that attributes are same and our struct still compatible with global shm_mq */
	Assert(shm_mq_get_sender(mq) == amq->mq_sender);
	Assert(shm_mq_get_receiver(mq) == amq->mq_receiver);

	amq->mq_sender = NULL;
	amq->mq_detached = false;
}

static void
shm_mq_clean_receiver(shm_mq *mq)
{
	struct shm_mq_alt	*amq = (struct shm_mq_alt *) mq;

	/* check that attributes are same and our struct still compatible with global shm_mq */
	Assert(shm_mq_get_sender(mq) == amq->mq_sender);
	Assert(shm_mq_get_receiver(mq) == amq->mq_receiver);

	amq->mq_receiver = NULL;
	amq->mq_detached = false;
}

static size_t
jsonbc_shmem_size(void)
{
	int					i;
	shm_toc_estimator	e;
	Size				size;

	Assert(jsonbc_nworkers != -1);
	shm_toc_initialize_estimator(&e);

	shm_toc_estimate_chunk(&e, sizeof(jsonbc_shm_hdr));
	for (i = 0; i < jsonbc_nworkers; i++)
	{
		shm_toc_estimate_chunk(&e, sizeof(jsonbc_shm_worker));
		shm_toc_estimate_chunk(&e, jsonbc_get_queue_size());
		shm_toc_estimate_chunk(&e, jsonbc_get_queue_size());
	}
	shm_toc_estimate_keys(&e, jsonbc_nworkers * 3 + 1);
	size = shm_toc_estimate(&e);
	return size;
}

static void
jsonbc_shmem_startup_hook(void)
{
	int				mqkey;
	bool			found;
	Size			size = jsonbc_shmem_size();
	jsonbc_shm_hdr *hdr;

	/* Invoke original hook if needed */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	workers_data = ShmemInitStruct("jsonbc workers shmem", size, &found);

	if (!found)
	{
		int i;

		toc = shm_toc_create(JSONBC_SHM_MQ_MAGIC, workers_data, size);
		hdr = shm_toc_allocate(toc, sizeof(jsonbc_shm_hdr));
		hdr->workers_ready = 0;
		shm_toc_insert(toc, 0, hdr);
		mqkey = jsonbc_nworkers + 1;

		for (i = 0; i < jsonbc_nworkers; i++)
		{
			jsonbc_shm_worker	*wd = shm_toc_allocate(toc, sizeof(jsonbc_shm_worker));

			/* each worker will have two mq, for input and output */
			wd->mqin = shm_mq_create(shm_toc_allocate(toc, jsonbc_get_queue_size()),
							   jsonbc_get_queue_size());
			wd->mqout = shm_mq_create(shm_toc_allocate(toc, jsonbc_get_queue_size()),
							   jsonbc_get_queue_size());

			/* init worker context */
			pg_atomic_init_flag(&wd->busy);
			wd->proc = NULL;

			shm_toc_insert(toc, i + 1, wd);
			shm_toc_insert(toc, mqkey++, wd->mqin);
			shm_toc_insert(toc, mqkey++, wd->mqout);
		}
	}
	else toc = shm_toc_attach(JSONBC_SHM_MQ_MAGIC, workers_data);

	LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errmsg("jsonbc module must be initialized in postmaster."),
				 errhint("add 'jsonbc' to shared_preload_libraries parameter in postgresql.conf")));
	}

	setup_guc_variables();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = jsonbc_shmem_startup_hook;

	if (jsonbc_nworkers)
	{
		int i;
		RequestAddinShmemSpace(jsonbc_shmem_size());
		for (i = 0; i < jsonbc_nworkers; i++)
			jsonbc_register_worker(i);
	}
	else elog(LOG, "jsonbc: workers are disabled");
}

static void
setup_guc_variables(void)
{
	DefineCustomIntVariable("jsonbc.workers_count",
							"Count of workers for jsonbc compresssion",
							NULL,
							&jsonbc_nworkers,
							1, /* default */
							0, /* if zero then no workers */
							MAX_JSONBC_WORKERS,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("jsonbc.cache_size",
							"Cache size for each compression options (kilobytes)",
							NULL,
							&jsonbc_cache_size,
							1, /* 1kb by default */
							0,	/* no cache */
							1024, /* 1 mb */
							PGC_SUSET,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("jsonbc.queue_size",
							"Size of queue used for communication with workers (kilobytes)",
							NULL,
							&jsonbc_queue_size,
							1, /* 1kb by default */
							1,	/* 1 kb is minimum too */
							1024, /* 1 mb */
							PGC_SUSET,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);
}

typedef struct ids_callback_state
{
	uint32 *idsbuf;
	int		nkeys;
} ids_callback_state;

static bool
ids_callback(char *res, size_t reslen, void *arg)
{
	ids_callback_state *state;

	/*
	 * when there is at least one key, reslen will be >= sizeof(uint32),
	 * so we can safely use 1 as indicator if something went wrong
	 */
	if (reslen == 1)
		return false;

	/* size of the received data should be equal to key array size */
	state = (ids_callback_state *) arg;
	Assert(reslen == sizeof(uint32) * state->nkeys);
	memcpy((void *) state->idsbuf, res, reslen);
	return true;
}

typedef struct
{
	char **keys;
	int	   nkeys;
} keys_callback_state;

static bool
keys_callback(char *res, size_t reslen, void *arg)
{
	int		i = 0,
			nkeys = 0;

	keys_callback_state *state = (keys_callback_state *) arg;

	/* it should at least two symbols in the response */
	if (reslen == 1)
		return false;

	/* increase the global buffer if we need to */
	if (reslen > compression_buffers->buflen)
	{
		compression_buffers->buf =
			(char *) repalloc(compression_buffers->buf, reslen);
		compression_buffers->buflen = reslen;
	}

	memcpy(compression_buffers->buf, res, reslen);

	while (reslen--)
	{
		if (res[i] != '\0')
			state->keys[nkeys++] = &compression_buffers->buf[i];
		i++;
	}

	return (nkeys == state->nkeys);
}

static void
jsonbc_communicate(shm_mq_iovec *iov, int iov_len,
		bool (*callback)(char *, size_t, void *), void *callback_arg)
{
	int					i;
	bool				detached = false;
	bool				callback_succeded = false;
	shm_mq_result		resmq;
	shm_mq_handle	   *mqh;

	char			   *res;
	Size				reslen;

	if (jsonbc_nworkers <= 0)
		/* TODO: maybe add support of multiple databases for dictionaries */
		elog(ERROR, "jsonbc workers are not available");

again:
	for (i = 0; i < jsonbc_nworkers; i++)
	{
		jsonbc_shm_worker *wd = shm_toc_lookup(toc, i + 1, false);
		if (!pg_atomic_test_set_flag(&wd->busy))
			continue;

		/* send data */
		shm_mq_set_sender(wd->mqin, MyProc);
		mqh = shm_mq_attach(wd->mqin, NULL, NULL);
		resmq = shm_mq_sendv(mqh, iov, iov_len, false);
		if (resmq != SHM_MQ_SUCCESS)
			detached = true;
		shm_mq_detach(mqh);

		/*
		 * we need to clean sender before receiving, because we need to block
		 * worker after we received data
		 */
		shm_mq_clean_sender(wd->mqin);

		/* get data */
		if (!detached)
		{
			shm_mq_set_receiver(wd->mqout, MyProc);
			mqh = shm_mq_attach(wd->mqout, NULL, NULL);
			resmq = shm_mq_receive(mqh, &reslen, (void **) &res, false);
			if (resmq != SHM_MQ_SUCCESS)
				detached = true;

			if (!detached)
				callback_succeded = callback(res, reslen, callback_arg);

			shm_mq_detach(mqh);
		}

		/* clean self as receiver and unlock mq */
		shm_mq_clean_receiver(wd->mqout);
		pg_atomic_clear_flag(&wd->busy);

		if (detached)
			elog(ERROR, "jsonbc: worker has detached");

		if (!callback_succeded)
			elog(ERROR, "jsonbc: communication error");

		/* we're done here */
		return;
	}

	CHECK_FOR_INTERRUPTS();
	pg_usleep(100);

	/* TODO: add attempts count check */
	goto again;
}

/* Get key IDs using workers */
static void
jsonbc_get_key_ids(Oid cmoptoid, char *buf, int buflen, uint32 *idsbuf, int nkeys)
{
	JsonbcCommand		cmd = JSONBC_CMD_GET_IDS;
	shm_mq_iovec		iov[4];
	ids_callback_state	state;

	iov[0].data = (void *) &nkeys;
	iov[0].len = sizeof(int);

	iov[1].data = (void *) &cmoptoid;
	iov[1].len = sizeof(Oid);

	iov[2].data = (void *) &cmd;
	iov[2].len = sizeof(JsonbcCommand);

	iov[3].data = buf;
	iov[3].len = buflen;

	state.idsbuf = idsbuf;
	state.nkeys = nkeys;
	jsonbc_communicate(iov, 4, ids_callback, &state);
}

/* Get keys by their IDs using workers */
static void
jsonbc_get_keys(Oid cmoptoid, uint32 *ids, int nkeys, char **keys)
{
	JsonbcCommand		cmd = JSONBC_CMD_GET_KEYS;
	shm_mq_iovec		iov[4];
	keys_callback_state	state;

	iov[0].data = (void *) &nkeys;
	iov[0].len = sizeof(int);

	iov[1].data = (void *) &cmoptoid;
	iov[1].len = sizeof(Oid);

	iov[2].data = (void *) &cmd;
	iov[2].len = sizeof(JsonbcCommand);

	iov[3].data = (char *) ids;
	iov[3].len = sizeof(uint32) * nkeys;

	state.keys = keys;
	state.nkeys = nkeys;
	jsonbc_communicate(iov, 4, keys_callback, &state);
}

/*
 * Varbyte-encode 'val' into *ptr.
 */
static void
encode_varbyte(uint32 val, unsigned char *ptr, int *len)
{
	unsigned char *p = ptr;

	while (val > 0x7F)
	{
		*(p++) = 0x80 | (val & 0x7F);
		val >>= 7;
	}
	*(p++) = (unsigned char) val;
	*len = p - ptr;
}

/*
 * Decode varbyte-encoded integer at *ptr.
 */
static uint32
decode_varbyte(unsigned char *ptr)
{
	uint32		val;
	unsigned char *p = ptr;
	uint32		c;

	c = *(p++);
	val = c & 0x7F;
	if (c & 0x80)
	{
		c = *(p++);
		val |= (c & 0x7F) << 7;
		if (c & 0x80)
		{
			c = *(p++);
			val |= (c & 0x7F) << 14;
			if (c & 0x80)
			{
				c = *(p++);
				val |= (c & 0x7F) << 21;
				if (c & 0x80)
				{
					c = *(p++);
					val |= c << 28;
				}
			}
		}
	}

	return val;
}

static void
init_memory_context(bool init_buffers)
{
	MemoryContext			old_mcxt;
	MemoryContextCallback  *cb;

	if (compression_mcxt)
		return;

	compression_mcxt = AllocSetContextCreate(TopTransactionContext,
											 "jsonbc compression context",
											 ALLOCSET_DEFAULT_SIZES);
	cb = MemoryContextAlloc(TopTransactionContext,
							sizeof(MemoryContextCallback));
	cb->func = memory_reset_callback;
	cb->arg = NULL;

	MemoryContextRegisterResetCallback(TopTransactionContext, cb);

	if (init_buffers)
	{
		old_mcxt = MemoryContextSwitchTo(compression_mcxt);
		compression_buffers = palloc(sizeof(CompressionThroughBuffers));
		compression_buffers->buflen = 1024;
		compression_buffers->idslen = 256;
		compression_buffers->buf = palloc(compression_buffers->buflen);
		compression_buffers->idsbuf =
				(uint32 *) palloc(compression_buffers->idslen * sizeof(uint32));
		MemoryContextSwitchTo(old_mcxt);

		compression_buffers->item_mcxt = AllocSetContextCreate(compression_mcxt,
												 "jsonbc item context",
												 ALLOCSET_DEFAULT_SIZES);
	}
}

static void
memory_reset_callback(void *arg)
{
	compression_mcxt = NULL;
}

/*
 * Given a JsonbValue, convert to Jsonb but with different header part.
 * The result is palloc'd.
 * It adds a space for header, that should be filled later.
 */
#ifdef PGPRO_JSONBC
static char *
packJsonbValue(JsonbValue *val, int header_size, int *len)
{
	StringInfoData buffer;
	JEntry		jentry;

	/* Should not already have binary representation */
	Assert(val->type == jbvObject || val->type == jbvArray);

	/* Allocate an output buffer. It will be enlarged as needed */
	initStringInfo(&buffer);

	/* Make room for the varlena header */
	enlargeStringInfo(&buffer, header_size);
	buffer.len = header_size;
	buffer.data[buffer.len] = '\0';

	ConvertJsonbValue(&buffer, &jentry, val, 0);

	/*
	 * Note: the JEntry of the root is discarded. Therefore the root
	 * JsonbContainer struct must contain enough information to tell what kind
	 * of value it is.
	 */

	*len = buffer.len;
	return buffer.data;
}
#else
static char *
packJsonbValue(JsonbValue *val, int header_size, int *len)
{
	Jsonb  *jb = JsonbValueToJsonb(val);
	int		size = VARSIZE(jb);

	if (header_size != VARHDRSZ)
	{
		Assert(header_size > VARHDRSZ);
		*len = size + (header_size - VARHDRSZ);
		jb = (Jsonb *) repalloc((void *) jb, *len);
		memmove((char *)jb + header_size, (char *)jb + VARHDRSZ, size - VARHDRSZ);
		memset((char *) jb, 0, header_size);
	}
	return (char *) jb;
}
#endif

/* Compress jsonb using dictionary */
static struct varlena *
jsonbc_compress(AttributeCompression *ac, const struct varlena *data)
{
	int					size;
	JsonbIteratorToken	r;
	JsonbValue			jv;
	JsonbIterator	   *it;
	JsonbValue		   *jbv = NULL;
	JsonbParseState	   *state = NULL;
	struct varlena	   *res;

	init_memory_context(true);

	it = JsonbIteratorInit(&((Jsonb *) data)->root);
	while ((r = JsonbIteratorNext(&it, &jv, false)) != 0)
	{
		/* we assume that jsonb has already been sorted and uniquefied */
		jbv = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &jv : NULL);

		if (r == WJB_END_OBJECT && jbv->type == jbvObject)
		{
			int		i,
					len,
					nkeys = jbv->val.object.nPairs,
					offset = 0;

			/* maximum length of encoded uint32 is 5 */
			char   *keyptr = MemoryContextAlloc(compression_buffers->item_mcxt, nkeys * 5),
				   *buf;
			uint32 *idsbuf;

			/* increase the size of buffer for key ids if we need to */
			if (nkeys > compression_buffers->idslen)
			{
				compression_buffers->idsbuf =
					(uint32 *) repalloc(compression_buffers->idsbuf, nkeys * sizeof(uint32));
				compression_buffers->idslen = nkeys;
			}

			/* calculate length of keys */
			len = 0;
			for (i = 0; i < nkeys; i++)
			{
				JsonbValue *v = &jbv->val.object.pairs[i].key;
				len += v->val.string.len + 1 /* \0 */;
			}

			/* increase the buffer if we need to */
			if (len > compression_buffers->buflen)
			{
				compression_buffers->buf =
					(char *) repalloc(compression_buffers->buf, len);
				compression_buffers->buflen = len;
			}

			/* copy all keys to buffer */
			buf = compression_buffers->buf;
			idsbuf = compression_buffers->idsbuf;

			for (i = 0; i < nkeys; i++)
			{
				JsonbValue *v = &jbv->val.object.pairs[i].key;
				memcpy(buf + offset, v->val.string.val, v->val.string.len);
				offset += v->val.string.len;
				buf[offset++] = '\0';
			}

			Assert(offset == len);

			/* retrieve or generate ids */
			jsonbc_get_key_ids(ac->cmoptoid, buf, len, idsbuf, nkeys);

			/* replace the old keys with encoded ids */
			for (i = 0; i < nkeys; i++)
			{
				int keylen;

				JsonbValue *v = &jbv->val.object.pairs[i].key;

				encode_varbyte(idsbuf[i], (unsigned char *) keyptr, &keylen);
				v->val.string.val = keyptr;
				v->val.string.len = keylen;
				keyptr += keylen;
			}
		}
	}

	/* don't compress scalar values */
	if (jbv == NULL || IsAJsonbScalar(jbv))
		return NULL;

	res = (struct varlena *) packJsonbValue(jbv, VARHDRSZ_CUSTOM_COMPRESSED, &size);
	SET_VARSIZE_COMPRESSED(res, size);

	MemoryContextReset(compression_buffers->item_mcxt);
	return res;
}

static void
jsonbc_configure(Form_pg_attribute attr, List *options)
{
	if (options != NIL)
		elog(ERROR, "the compression method for jsonbc doesn't take any options");
}

static struct varlena *
jsonbc_decompress(AttributeCompression *ac, const struct varlena *data)
{
	JsonbIteratorToken	r;
	JsonbValue			v,
					   *jbv = NULL;
	JsonbIterator	   *it;
	Jsonb			   *jb;
	JsonbParseState	   *state = NULL;
	struct varlena	   *res;

	init_memory_context(false);
	Assert(VARATT_IS_CUSTOM_COMPRESSED(data));

	jb = (Jsonb *) ((char *) data + VARHDRSZ_CUSTOM_COMPRESSED - offsetof(Jsonb, root));
	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		jbv = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

		if (r == WJB_END_OBJECT && jbv->type == jbvObject)
		{
			char  **keys;
			int		i,
					nkeys = jbv->val.object.nPairs;

			/* increase the size of buffer for key ids if we need to */
			if (nkeys > compression_buffers->idslen)
			{
				compression_buffers->idsbuf =
					(uint32 *) repalloc(compression_buffers->idsbuf, nkeys * sizeof(uint32));
				compression_buffers->idslen = nkeys;
			}

			/* decode keys */
			for (i = 0; i < nkeys; i++)
			{
				int32		key_id;
				JsonbValue *v = &jbv->val.object.pairs[i].key;

				Assert(v->type == jbvString);
				Assert(v->val.string.len <= 5);
				key_id = decode_varbyte((unsigned char *) v->val.string.val);
				compression_buffers->idsbuf[i] = key_id;
			}

			/* retrieve or generate ids */
			keys = (char **) palloc(sizeof(char *) * nkeys);
			jsonbc_get_keys(ac->cmoptoid, compression_buffers->idsbuf, nkeys, keys);

			/* replace the encoded keys with real keys */
			for (i = 0; i < nkeys; i++)
			{
				JsonbValue *v = &jbv->val.object.pairs[i].key;
				v->val.string.val = keys[i];
				v->val.string.len = strlen(keys[i]);
			}
			pfree(keys);
		}
	}

	res = (struct varlena *) JsonbValueToJsonb(jbv);
	return res;
}

Datum
jsonbc_compression_handler(PG_FUNCTION_ARGS)
{
	CompressionMethodRoutine   *cmr = makeNode(CompressionMethodRoutine);
	CompressionMethodOpArgs	   *opargs =
		(CompressionMethodOpArgs *) PG_GETARG_POINTER(0);
	Oid							typeid = opargs->typeid;

	if (OidIsValid(typeid) && typeid != JSONBOID)
		elog(ERROR, "unexpected type %d for jsonbc compression handler", typeid);

	cmr->configure = jsonbc_configure;
	cmr->drop = NULL;
	cmr->compress = jsonbc_compress;
	cmr->decompress = jsonbc_decompress;

	PG_RETURN_POINTER(cmr);
}
