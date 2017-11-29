#include "jsonbd.h"
#include "jsonbd_utils.h"

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
PG_FUNCTION_INFO_V1(jsonbd_compression_handler);

/* we use one buffer for whole transaction to avoid extra allocations */
typedef struct
{
	char	   *buf;		/* keys */
	int			buflen;
	uint32	   *idsbuf;		/* key ids */
	int			idslen;

	MemoryContext	item_mcxt;
} CompressionThroughBuffers;

/* local */
static MemoryContext compression_mcxt = NULL;
static CompressionThroughBuffers *compression_buffers = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static shm_toc *toc = NULL;

/* global */
void *workers_data = NULL;
int jsonbd_nworkers = -1;
int jsonbd_queue_size = 0;

static void init_memory_context(bool);
static void memory_reset_callback(void *arg);
static void encode_varbyte(uint32 val, unsigned char *ptr, int *len);
static char *packJsonbValue(JsonbValue *val, int header_size, int *len);
static void setup_guc_variables(void);
static char *jsonbd_worker_get_keys(Oid cmoptoid, uint32 *ids, int nkeys, size_t *buflen);
static void jsonbd_worker_get_key_ids(Oid cmoptoid, char *buf, int buflen, uint32 *idsbuf, int nkeys);
static uint32 decode_varbyte(unsigned char *ptr);

static size_t
jsonbd_get_queue_size(void)
{
	return (Size) (shm_mq_minimum_size + jsonbd_queue_size * 1024);
}

static size_t
jsonbd_shmem_size(void)
{
	int					i;
	shm_toc_estimator	e;
	Size				size;

	Assert(jsonbd_nworkers > 0);
	shm_toc_initialize_estimator(&e);

	shm_toc_estimate_chunk(&e, sizeof(jsonbd_shm_hdr));

	/* two queues for launcher */
	shm_toc_estimate_chunk(&e, shm_mq_minimum_size * 2);

	for (i = 0; i < MAX_JSONBD_WORKERS; i++)
	{
		shm_toc_estimate_chunk(&e, sizeof(jsonbd_shm_worker));
		shm_toc_estimate_chunk(&e, jsonbd_get_queue_size());
		shm_toc_estimate_chunk(&e, jsonbd_get_queue_size());
	}

	/* 3 keys each worker + 3 for header (header itself and two queues) */
	shm_toc_estimate_keys(&e, MAX_JSONBD_WORKERS * 3 + 3);
	size = shm_toc_estimate(&e);
	return size;
}

/*
 * Initialize worker shm block
 *
 * About keys in toc:
 *	0 - for header
 *	1..MAX_JSONBD_WORKERS - workers
 *	MAX_JSONBD_WORKERS + 1 .. - queues
 */
static void
jsonbd_init_worker(shm_toc *toc, jsonbd_shm_worker *wd, int worker_num,
		size_t queue_size)
{
	LWLockPadded	*locks;
	static int mqkey = MAX_JSONBD_WORKERS + 1;

	/* each worker will have two mq, for input and output */
	wd->mqin = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);
	wd->mqout = shm_mq_create(shm_toc_allocate(toc, queue_size), queue_size);

	/* init worker context */
	wd->proc = NULL;
	wd->dboid = InvalidOid;

	shm_mq_clean_receiver(wd->mqin);
	shm_mq_clean_receiver(wd->mqout);
	shm_mq_clean_sender(wd->mqin);
	shm_mq_clean_sender(wd->mqout);

	if (worker_num)
		shm_toc_insert(toc, worker_num, wd);

	shm_toc_insert(toc, mqkey++, wd->mqin);
	shm_toc_insert(toc, mqkey++, wd->mqout);

	/* initialize worker's lwlock */
	locks = GetNamedLWLockTranche(JSONBD_LWLOCKS_TRANCHE);
	wd->lock = &locks[worker_num].lock;
}

static void
jsonbd_shmem_startup_hook(void)
{
	bool			found;
	Size			size = jsonbd_shmem_size();
	jsonbd_shm_hdr *hdr;

	/* Invoke original hook if needed */
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	workers_data = ShmemInitStruct("jsonbd workers shmem", size, &found);

	if (!found)
	{
		int i;
		jsonbd_shm_worker	*wd;
		size_t queue_size = jsonbd_get_queue_size();

		toc = shm_toc_create(JSONBD_SHM_MQ_MAGIC, workers_data, size);

		/* Initialize header */
		hdr = shm_toc_allocate(toc, sizeof(jsonbd_shm_hdr));
		hdr->workers_ready = 0;
		jsonbd_init_worker(toc, &hdr->launcher, 0, shm_mq_minimum_size);
		shm_toc_insert(toc, 0, hdr);

		for (i = 0; i < MAX_JSONBD_WORKERS; i++)
		{
			wd = shm_toc_allocate(toc, sizeof(jsonbd_shm_worker));
			jsonbd_init_worker(toc, wd, i + 1, queue_size);
		}
	}
	else toc = shm_toc_attach(JSONBD_SHM_MQ_MAGIC, workers_data);

	LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errmsg("jsonbd module must be initialized in postmaster."),
				 errhint("add 'jsonbd' to shared_preload_libraries parameter in postgresql.conf")));
	}

	setup_guc_variables();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = jsonbd_shmem_startup_hook;

	if (jsonbd_nworkers)
	{
		/* jsonbd workers and one lwlock for launcher */
		RequestNamedLWLockTranche(JSONBD_LWLOCKS_TRANCHE, MAX_JSONBD_WORKERS + 1);
		RequestAddinShmemSpace(jsonbd_shmem_size());
		jsonbd_register_launcher();
	}
	else elog(LOG, "jsonbd: workers are disabled");
}

static void
setup_guc_variables(void)
{
	DefineCustomIntVariable("jsonbd.workers_count",
							"Count of workers for jsonbd compresssion",
							NULL,
							&jsonbd_nworkers,
							1, /* default */
							0, /* if zero then no workers */
							MAX_JSONBD_WORKERS,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("jsonbd.queue_size",
							"Size of queue used for communication with workers (kilobytes)",
							NULL,
							&jsonbd_queue_size,
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
	size_t	 buflen;
	char	*buf;
} keys_callback_state;

static bool
keys_callback(char *res, size_t reslen, void *arg)
{
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

	/* save the received data */
	memcpy(compression_buffers->buf, res, reslen);

	state->buf = compression_buffers->buf;
	state->buflen = reslen;

	return true;
}

static void
jsonbd_communicate(shm_mq_iovec *iov, int iov_len,
		bool (*callback)(char *, size_t, void *), void *callback_arg)
{
	int					i,
						j;
	bool				detached = false;
	bool				launch_failed = false;
	bool				callback_succeded = false;
	shm_mq_result		resmq;
	shm_mq_handle	   *mqh;
	jsonbd_shm_hdr	   *hdr;
	jsonbd_shm_worker  *wd = NULL;

	char			   *res;
	Size				reslen;

	if (jsonbd_nworkers <= 0)
		elog(ERROR, "jsonbd workers are not available");

	hdr = shm_toc_lookup(toc, 0, false);

	/*
	 * find some not busy worker,
	 * the backend can intercept a worker that just started by another
	 * backend, that's ok
	 */
	while (true)
	{
		for (i = 0; i < hdr->workers_ready; i++)
		{
			bool locked;

			wd = shm_toc_lookup(toc, i + 1, false);
			if (wd->dboid != MyDatabaseId)
				continue;

			/*
			 * we found first worker for our database, next 'jsonbd_nworkers'
			 * workers should be ours
			 */
			locked = LWLockConditionalAcquire(wd->lock, LW_EXCLUSIVE);
			if (locked)
				goto comm;

			for (j = i; j < (i + jsonbd_nworkers); j++)
			{
				wd = shm_toc_lookup(toc, j + 1, false);

				if (wd->dboid != MyDatabaseId)
					/* somehow not all workers started for this database, try next */
					continue;

				locked = LWLockConditionalAcquire(wd->lock, LW_EXCLUSIVE);
				if (locked)
					goto comm;
			}

			/* if none of the workers were free, we just wait on last one */
			Assert(!locked);
			LWLockAcquire(wd->lock, LW_EXCLUSIVE);
			goto comm;
		}

		/*
		 * There are no workers for our database,
		 * so we should launch them using our jsonbd workers launcher
		 *
		 * But if the launcher already locked, we should check the workers
		 * list again
		 */
		if (!LWLockAcquireOrWait(hdr->launcher.lock, LW_EXCLUSIVE))
			continue;

		shm_mq_set_sender(hdr->launcher.mqin, MyProc);
		shm_mq_set_receiver(hdr->launcher.mqout, MyProc);

		mqh = shm_mq_attach(hdr->launcher.mqin, NULL, NULL);
		resmq = shm_mq_sendv(mqh,
				&((shm_mq_iovec) {(char *) &MyDatabaseId, sizeof(MyDatabaseId)}), 1, false);
		if (resmq != SHM_MQ_SUCCESS)
			detached = true;
		shm_mq_detach(mqh);

		if (!detached)
		{
			mqh = shm_mq_attach(hdr->launcher.mqout, NULL, NULL);
			resmq = shm_mq_receive(mqh, &reslen, (void **) &res, false);
			if (resmq != SHM_MQ_SUCCESS)
				detached = true;

			if (reslen != 2 || res[0] == 'n')
				launch_failed = true;

			shm_mq_detach(mqh);
		}

		shm_mq_clean_sender(hdr->launcher.mqin);
		shm_mq_clean_receiver(hdr->launcher.mqout);
		LWLockRelease(hdr->launcher.lock);

		if (detached)
			elog(ERROR, "jsonbd: workers launcher was detached");

		if (launch_failed)
			elog(ERROR, "jsonbd: could not launch dictionary workers, see logs");
	}

comm:
	Assert(wd != NULL);

	detached = false;

	/* send data */
	shm_mq_set_sender(wd->mqin, MyProc);
	shm_mq_set_receiver(wd->mqout, MyProc);

	mqh = shm_mq_attach(wd->mqin, NULL, NULL);
	resmq = shm_mq_sendv(mqh, iov, iov_len, false);
	if (resmq != SHM_MQ_SUCCESS)
		detached = true;
	shm_mq_detach(mqh);

	/* get data */
	if (!detached)
	{
		mqh = shm_mq_attach(wd->mqout, NULL, NULL);
		resmq = shm_mq_receive(mqh, &reslen, (void **) &res, false);
		if (resmq != SHM_MQ_SUCCESS)
			detached = true;

		if (!detached)
			callback_succeded = callback(res, reslen, callback_arg);

		shm_mq_detach(mqh);
	}

	/* clean self as receiver and unlock mq */
	shm_mq_clean_sender(wd->mqin);
	shm_mq_clean_receiver(wd->mqout);

	LWLockRelease(wd->lock);

	if (detached)
		elog(ERROR, "jsonbd: worker has detached");

	if (!callback_succeded)
		elog(ERROR, "jsonbd: communication error");

}

/* Get key IDs using workers */
static void
jsonbd_worker_get_key_ids(Oid cmoptoid, char *buf, int buflen, uint32 *idsbuf, int nkeys)
{
	JsonbcCommand		cmd = JSONBD_CMD_GET_IDS;
	shm_mq_iovec		iov[4];
	ids_callback_state	state;

	iov[0].data = (void *) &nkeys;
	iov[0].len = sizeof(nkeys);

	iov[1].data = (void *) &cmoptoid;
	iov[1].len = sizeof(cmoptoid);

	iov[2].data = (void *) &cmd;
	iov[2].len = sizeof(cmd);

	iov[3].data = buf;
	iov[3].len = buflen;

	state.idsbuf = idsbuf;
	state.nkeys = nkeys;
	jsonbd_communicate(iov, 4, ids_callback, &state);
}

/* Get keys by their IDs using workers */
static char *
jsonbd_worker_get_keys(Oid cmoptoid, uint32 *ids, int nkeys, size_t *buflen)
{
	JsonbcCommand		cmd = JSONBD_CMD_GET_KEYS;
	shm_mq_iovec		iov[4];
	keys_callback_state	state;

	iov[0].data = (void *) &nkeys;
	iov[0].len = sizeof(nkeys);

	iov[1].data = (void *) &cmoptoid;
	iov[1].len = sizeof(cmoptoid);

	iov[2].data = (void *) &cmd;
	iov[2].len = sizeof(cmd);

	iov[3].data = (char *) ids;
	iov[3].len = sizeof(uint32) * nkeys;

	state.buf = NULL;
	state.buflen = 0;
	jsonbd_communicate(iov, 4, keys_callback, &state);

	*buflen = state.buflen;
	return state.buf;
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
											 "jsonbd compression context",
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
												 "jsonbd item context",
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
#ifdef PGPRO_JSONBD
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
jsonbd_compress(CompressionMethodOptions *cmoptions, const struct varlena *data)
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
			jsonbd_worker_get_key_ids(cmoptions->cmoptoid, buf, len, idsbuf, nkeys);

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
jsonbd_configure(Form_pg_attribute attr, List *options)
{
	if (options != NIL)
		elog(ERROR, "the compression method for jsonbd doesn't take any options");
}

static struct varlena *
jsonbd_decompress(CompressionMethodOptions *cmoptions, const struct varlena *data)
{
	JsonbIteratorToken	r;
	JsonbValue			v,
					   *jbv = NULL;
	JsonbIterator	   *it;
	Jsonb			   *jb;
	JsonbParseState	   *state = NULL;
	struct varlena	   *res;

	init_memory_context(true);
	Assert(VARATT_IS_CUSTOM_COMPRESSED(data));

	jb = (Jsonb *) ((char *) data + VARHDRSZ_CUSTOM_COMPRESSED - offsetof(Jsonb, root));
	it = JsonbIteratorInit(&jb->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		jbv = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);

		if (r == WJB_END_OBJECT && jbv->type == jbvObject)
		{
			char   *buf;
			size_t	buflen,
					offset = 0;
			int		i,
					nkeys = jbv->val.object.nPairs;

			/* increase the size of buffer for key ids if we need to */
			if (nkeys > compression_buffers->idslen)
			{
				compression_buffers->idsbuf =
					(uint32 *) repalloc(compression_buffers->idsbuf, nkeys * sizeof(uint32));
				compression_buffers->idslen = nkeys;
			}

			/* decode key ids */
			for (i = 0; i < nkeys; i++)
			{
				int32		key_id;
				JsonbValue *v = &jbv->val.object.pairs[i].key;

				Assert(v->type == jbvString);
				Assert(v->val.string.len <= 5);
				key_id = decode_varbyte((unsigned char *) v->val.string.val);
				compression_buffers->idsbuf[i] = key_id;
			}

			/* retrieve keys */
			buf = jsonbd_worker_get_keys(cmoptions->cmoptoid, compression_buffers->idsbuf, nkeys, &buflen);
			if (buf == NULL)
				elog(ERROR, "jsonbd: decompression error");

			/* replace the encoded keys with real keys */
			for (i = 0; i < nkeys; i++)
			{
				size_t		oldoff	= offset;
				JsonbValue *v		= &jbv->val.object.pairs[i].key;

				v->val.string.val = &buf[offset];

				/* move to next key in buffer */
				while (buf[offset++] != '\0')
					Assert(offset <= buflen);

				v->val.string.len = offset - oldoff - 1;
			}

			/* check correctness */
			Assert(offset == buflen);
		}
	}

	res = (struct varlena *) JsonbValueToJsonb(jbv);
	return res;
}

Datum
jsonbd_compression_handler(PG_FUNCTION_ARGS)
{
	CompressionMethodRoutine   *cmr = makeNode(CompressionMethodRoutine);
	CompressionMethodOpArgs	   *opargs =
		(CompressionMethodOpArgs *) PG_GETARG_POINTER(0);
	Oid							typeid = opargs->typeid;

	if (OidIsValid(typeid) && typeid != JSONBOID)
		elog(ERROR, "unexpected type %d for jsonbd compression handler", typeid);

	cmr->configure = jsonbd_configure;
	cmr->drop = NULL;
	cmr->compress = jsonbd_compress;
	cmr->decompress = jsonbd_decompress;

	PG_RETURN_POINTER(cmr);
}
