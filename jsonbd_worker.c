#include "jsonbd.h"
#include "jsonbd_utils.h"

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_compression_opt.h"
#include "catalog/pg_extension.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "executor/spi.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

static bool						xact_started = false;
static bool						shutdown_requested = false;
static jsonbd_shm_worker	   *worker_state;
static MemoryContext			worker_context = NULL;
static MemoryContext			worker_cache_context = NULL;
static HTAB					   *cmcache;

Oid jsonbd_dictionary_reloid	= InvalidOid;
Oid	jsonbd_keys_indoid			= InvalidOid;
Oid	jsonbd_id_indoid			= InvalidOid;

void jsonbd_worker_main(Datum arg);
void jsonbd_launcher_main(Datum arg);
static bool jsonbd_register_worker(int, Oid, int);
static char *jsonbd_get_dictionary_name(Oid relid);

#define JSONBD_DICTIONARY_REL	"jsonbd_dictionary"

static const char *sql_insert = \
	"WITH t AS (SELECT (COALESCE(MAX(id), 0) + 1) new_id FROM %s"
	" WHERE acoid = %d) INSERT INTO %s"
	" SELECT %d, t.new_id, '%s' FROM t RETURNING id";

enum {
	JSONBD_DICTIONARY_REL_ATT_ACOID = 1,
	JSONBD_DICTIONARY_REL_ATT_ID,
	JSONBD_DICTIONARY_REL_ATT_KEY,
	JSONBD_DICTIONARY_REL_ATT_COUNT
};

/*
 * Handle SIGTERM in BGW's process.
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	shutdown_requested = true;

	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/* Returns an item from compression options cache */
static jsonbd_cached_cmopt *
get_cached_compression_options(Oid cmoptoid)
{
	bool	found;
	jsonbd_cached_cmopt *cmdata;

	cmdata = hash_search(cmcache, &cmoptoid, HASH_ENTER, &found);
	if (!found)
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(uint32);
		hash_ctl.entrysize = sizeof(jsonbd_cached_key);
		hash_ctl.hcxt = worker_cache_context;

		cmdata->cmoptoid = cmoptoid;
		cmdata->key_cache = hash_create("jsonbd map by key",
							  128,
							  &hash_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		hash_ctl.entrysize = sizeof(jsonbd_cached_id);
		cmdata->id_cache = hash_create("jsonbd map by id",
							  128,
							  &hash_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return cmdata;
}

static void
init_worker(dsm_segment *seg)
{
	HASHCTL		hash_ctl;
	jsonbd_worker_args	*worker_args;

	shm_toc		   *toc = shm_toc_attach(JSONBD_SHM_MQ_MAGIC, workers_data);
	jsonbd_shm_hdr *hdr = shm_toc_lookup(toc, 0, false);

	worker_args = (jsonbd_worker_args *) dsm_segment_address(seg);

	/* Connect to our database */
	BackgroundWorkerInitializeConnectionByOid(worker_args->dboid, InvalidOid);

	worker_state = shm_toc_lookup(toc, worker_args->worker_num, false);
	worker_state->proc = MyProc;
	worker_state->dboid = worker_args->dboid;

	/* this context will be reset after each task */
	Assert(worker_context == NULL);
	worker_context = AllocSetContextCreate(TopMemoryContext,
										   "jsonbd worker context",
										   ALLOCSET_DEFAULT_SIZES);

	worker_cache_context = AllocSetContextCreate(TopMemoryContext,
										"jsonbd worker cache context",
										ALLOCSET_DEFAULT_SIZES);

	/* Initialize hash tables used to track update chains */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(jsonbd_cached_cmopt);
	hash_ctl.hcxt = worker_cache_context;

	cmcache = hash_create("jsonbd compression options cache",
						  128,		/* arbitrary initial size */
						  &hash_ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	elog(LOG, "jsonbd dictionary worker %d started with pid: %d",
			worker_args->worker_num, MyProcPid);

	/* We don't need this segment anymore */
	dsm_detach(seg);

	/* Set launcher free */
	SetLatch(&hdr->launcher_latch);
	InitLatch(&worker_state->latch);
	pg_atomic_init_flag(&worker_state->busy);

	/* make this worker visible in backend cycle */
	hdr->workers_ready++;
}

static void
start_xact_command(void)
{
	if (IsTransactionState())
		return;

	if (!xact_started)
	{
		ereport(DEBUG3,
				(errmsg_internal("StartTransactionCommand")));
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		xact_started = true;
	}
}

static void
finish_xact_command(void)
{
	if (xact_started)
	{
		/* Now commit the command */
		ereport(DEBUG3,
				(errmsg_internal("CommitTransactionCommand")));

		PopActiveSnapshot();
		CommitTransactionCommand();
		xact_started = false;
	}
}

static char *
jsonbd_get_key(Relation rel, Relation indrel, Oid cmoptoid, uint32 key_id)
{
	IndexScanDesc	scan;
	ScanKeyData		skey[2];
	Datum			key_datum;
	HeapTuple		tup;
	bool			isNull;
	MemoryContext	old_mcxt;
	char		   *res;

	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(cmoptoid));
	ScanKeyInit(&skey[1],
				2,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(key_id));

	scan = index_beginscan(rel, indrel, SnapshotAny, 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);

	tup = index_getnext(scan, ForwardScanDirection);
	if (tup == NULL)
		elog(ERROR, "key not found for cmopt=%d and id=%d", cmoptoid, key_id);

	key_datum = heap_getattr(tup, JSONBD_DICTIONARY_REL_ATT_KEY,
						  RelationGetDescr(rel), &isNull);
	Assert(!isNull);

	old_mcxt = MemoryContextSwitchTo(worker_context);
	res = TextDatumGetCString(key_datum);
	MemoryContextSwitchTo(old_mcxt);

	index_endscan(scan);
	return res;
}

/* Returns buffers with keys ordered by ids */
static char **
jsonbd_get_keys(Oid cmoptoid, uint32 *ids, int nkeys)
{
	int				i;
	char		  **keys;
	jsonbd_cached_cmopt		*cmcache;

	Oid			relid = jsonbd_get_dictionary_relid();
	Relation	rel = NULL,
				indrel;

	cmcache = get_cached_compression_options(cmoptoid);
	keys = (char **) MemoryContextAlloc(worker_context,
										sizeof(char *) * nkeys);

	for (i = 0; i < nkeys; i++)
	{
		bool		found;
		MemoryContext	oldcontext;
		jsonbd_cached_id		*cid;
		jsonbd_pair				*pair;

		Assert(cmcache->id_cache);
		cid = hash_search(cmcache->id_cache, &ids[i], HASH_ENTER, &found);

		if (found)
		{
			keys[i] = cid->pair->key;
			continue;
		}

		if (!rel)
		{
			start_xact_command();
			rel = relation_open(relid, AccessShareLock);
			indrel = index_open(jsonbd_id_indoid, AccessShareLock);
		}
		keys[i] = jsonbd_get_key(rel, indrel, cmoptoid, ids[i]);

		/* create new pair and save it in cache */
		oldcontext = MemoryContextSwitchTo(worker_cache_context);
		pair = (jsonbd_pair *) palloc(sizeof(jsonbd_pair));
		pair->id = ids[i];
		pair->key = pstrdup(keys[i]);
		cid->pair = pair;
		MemoryContextSwitchTo(oldcontext);
	}

	if (rel)
	{
		index_close(indrel, AccessShareLock);
		relation_close(rel, AccessShareLock);
		finish_xact_command();
	}

	return keys;
}

/*
 * Search for key in index.
 * Index should be locked properly
 */
static uint32
jsonbd_get_key_id(Relation rel, Relation indrel, Oid cmoptoid, char *key)
{
	IndexScanDesc	scan;
	ScanKeyData		skey[2];
	HeapTuple		tup;
	bool			isNull;
	uint32			result = 0;

	ScanKeyInit(&skey[0],
				1,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(cmoptoid));
	ScanKeyInit(&skey[1],
				2,
				BTEqualStrategyNumber,
				F_TEXTEQ,
				CStringGetTextDatum(key));

	scan = index_beginscan(rel, indrel, SnapshotAny, 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);

	tup = index_getnext(scan, ForwardScanDirection);
	if (tup != NULL)
	{
		Datum dat = heap_getattr(tup, JSONBD_DICTIONARY_REL_ATT_ID,
							  RelationGetDescr(rel), &isNull);
		Assert(!isNull);
		result = DatumGetInt32(dat);
	}
	index_endscan(scan);

	return result;
}

/*
 * Get key IDs using relation
 */
static bool
jsonbd_get_key_ids(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys)
{
	Relation	rel = NULL;
	Relation	indrel;
	int			i;
	Oid			relid = jsonbd_get_dictionary_relid();
	bool		spi_on = false,
				failed = false;
	jsonbd_cached_cmopt		*cmcache;
	static char *relname = NULL;

	if (relname == NULL)
	{
		start_xact_command();
		relname = jsonbd_get_dictionary_name(relid);
		finish_xact_command();
	}

	cmcache = get_cached_compression_options(cmoptoid);

	for (i = 0; i < nkeys; i++)
	{
		uint32		hkey;
		bool		found;
		MemoryContext	oldcontext;
		jsonbd_cached_key		*ckey;
		jsonbd_pair				*pair;

		hkey = qhashmurmur3_32(buf, strlen(buf));

		Assert(cmcache->key_cache);
		ckey = hash_search(cmcache->key_cache, &hkey, HASH_ENTER, &found);
		if (found)
		{
			ListCell	*lc;

			/* collisions check */
			foreach(lc, ckey->pairs)
			{
				jsonbd_pair	*pair = lfirst(lc);
				if (strcmp(pair->key, buf) == 0)
				{
					idsbuf[i] = pair->id;
					goto next;
				}
			}
		}
		else ckey->pairs = NIL;

		/* create new pair and save it in cache, id will be set after scan */
		oldcontext = MemoryContextSwitchTo(worker_cache_context);
		pair = (jsonbd_pair *) palloc(sizeof(jsonbd_pair));
		pair->key = pstrdup(buf);
		ckey->pairs = lappend(ckey->pairs, pair);
		MemoryContextSwitchTo(oldcontext);

		/* lazy transaction creation */
		if (!rel)
		{
			start_xact_command();
			rel = relation_open(relid, AccessShareLock);
			indrel = index_open(jsonbd_keys_indoid, AccessShareLock);
		}

		idsbuf[i] = jsonbd_get_key_id(rel, indrel, cmoptoid, buf);

		if (idsbuf[i] == 0)
		{
			Relation	indrel2;

			indrel2 = index_open(jsonbd_keys_indoid, ExclusiveLock);

			/* recheck, key could be added while we wait for lock */
			idsbuf[i] = jsonbd_get_key_id(rel, indrel2, cmoptoid, buf);

			if (idsbuf[i] == 0)
			{
				/* still need to add */
				Datum	datum;
				bool	isnull;
				char   *sql2 = psprintf(sql_insert, relname, cmoptoid,
										relname, cmoptoid, buf);

				/* TODO: maybe use bulk inserts instead of SPI */
				if (!spi_on)
				{
					/* lazy SPI initialization */
					if (SPI_connect() != SPI_OK_CONNECT)
					{
						failed = true;
						goto finish;
					}

					spi_on = true;
				}

				if (SPI_exec(sql2, 0) != SPI_OK_INSERT_RETURNING)
				{
					failed = true;
					goto finish;
				}

				pfree(sql2);

				datum = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc,
									  1,
									  &isnull);
				Assert(!isnull);
				idsbuf[i] = DatumGetInt32(datum);
			}
			index_close(indrel2, ExclusiveLock);
		}

		/* set correct id in cache */
		pair->id = idsbuf[i];
next:
		Assert(idsbuf[i] > 0);

		/* move to next key */
		while (*buf++ != '\0');
	}

finish:
	if (spi_on)
		SPI_finish();

	if (rel)
	{
		index_close(indrel, AccessShareLock);
		relation_close(rel, AccessShareLock);
	}

	if (failed && xact_started)
		AbortCurrentTransaction();
	else if (!failed)
		finish_xact_command();

	return !failed;
}

static char *
jsonbd_cmd_get_ids(int nkeys, Oid cmoptoid, char *buf, size_t *buflen)
{
	bool			ok;
	uint32		   *idsbuf;
	MemoryContext	old_mcxt = CurrentMemoryContext;;

	*buflen = nkeys * sizeof(uint32);
	idsbuf = (uint32 *) palloc(*buflen);
	ok = jsonbd_get_key_ids(cmoptoid, buf, idsbuf, nkeys);
	if (!ok)
	{
		elog(LOG, "jsonbd: cannot get ids");

		idsbuf[0] = 0;
		*buflen = 1;
	}

	MemoryContextSwitchTo(old_mcxt);
	return (char *) idsbuf;
}

static char **
jsonbd_cmd_get_keys(int nkeys, Oid cmoptoid, uint32 *ids)
{
	char		  **keys = NULL;
	MemoryContext	mcxt = CurrentMemoryContext;;

	PG_TRY();
	{
		keys = jsonbd_get_keys(cmoptoid, ids, nkeys);
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(mcxt);
		error = CopyErrorData();
		elog(LOG, "jsonbd: error occured: %s", error->message);
		FlushErrorState();
		pfree(error);
	}
	PG_END_TRY();

	return keys;
}

void
jsonbd_launcher_main(Datum arg)
{
	shm_toc			*toc;
	jsonbd_shm_hdr	*hdr;

	shm_mq_handle  *mqh;
	int				worker_num	= 1;
	int				database_num = 0;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Init this launcher as backend so workers can notify it */
	InitPostgres(NULL, InvalidOid, NULL, InvalidOid, NULL);

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbd_launcher_main");

	/* Init launcher state */
	Assert(workers_data != NULL);
	toc = shm_toc_attach(JSONBD_SHM_MQ_MAGIC, workers_data);
	hdr = shm_toc_lookup(toc, 0, false);
	worker_state = &hdr->launcher;
	worker_state->proc = MyProc;

	InitLatch(&hdr->launcher_latch);

	elog(LOG, "jsonbd launcher started with pid: %d", MyProcPid);

	while (true)
	{
		int		rc;
		Size	nbytes;
		void   *data;

		shm_mq_result	resmq;

		if (shutdown_requested)
			break;

		/* Wait to be signalled. */
		rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
					   0, PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		/* Reset the latch so we don't spin. */
		ResetLatch(MyLatch);

		if (shm_mq_get_sender(worker_state->mqin) == NULL)
		{
			CHECK_FOR_INTERRUPTS();
			continue;
		}

		/*
		 * set myself as receiver on mqin and sender on mqout,
		 * and get data from backend
		 * */
		if (!shm_mq_get_sender(worker_state->mqout))
			shm_mq_set_sender(worker_state->mqout, MyProc);

		if (!shm_mq_get_receiver(worker_state->mqin))
			shm_mq_set_receiver(worker_state->mqin, MyProc);

		mqh = shm_mq_attach(worker_state->mqin, NULL, NULL);
		resmq = shm_mq_receive(mqh, &nbytes, &data, false);

		if (resmq == SHM_MQ_DETACHED)
		{
			shm_mq_detach(mqh);
			continue;
		}

		if (resmq == SHM_MQ_SUCCESS)
		{
			int		started = 0;
			int		i;
			Oid		dboid;

			if (database_num >= MAX_DATABASES)
				elog(NOTICE, "jsonbd: reached maximum count of supported databases");
			else
			{
				Assert(nbytes == sizeof(Oid));
				dboid = *((Oid *) data);

				/* start workers for specified database */
				for (i=0; i < jsonbd_nworkers; i++, worker_num++)
				{
					bool res;

					res = jsonbd_register_worker(worker_num, dboid, database_num);
					if (res)
						started++;
				}
			}

			shm_mq_detach(mqh);

			mqh = shm_mq_attach(worker_state->mqout, NULL, NULL);
			if (started)
			{
				if (started != jsonbd_nworkers)
					elog(NOTICE, "jsonbd: not all workers for %d has started", dboid);

				/* we report ok if at least one worker has started */
				resmq = shm_mq_sendv(mqh, &((shm_mq_iovec) {"y", 2}), 1, false);
				database_num += 1;
			}
			else
				resmq = shm_mq_sendv(mqh, &((shm_mq_iovec) {"n", 2}), 1, false);
		}
		if (resmq != SHM_MQ_SUCCESS)
			elog(NOTICE, "jsonbd: backend detached early");

		/* shm_mq_detach(mqh); */
	}

	elog(LOG, "jsonbd launcher has ended its work");
	proc_exit(0);
}

void
jsonbd_worker_main(Datum arg)
{
	dsm_segment			*seg;
	shm_mq_handle		*mqh = NULL;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbd_worker");

	/* Initialize connection and local variables */
	seg = dsm_attach((dsm_handle) DatumGetInt32(arg));
	if (!seg)
		goto finish;

	init_worker(seg);

	MemoryContextSwitchTo(worker_context);

	while (true)
	{
		int		rc;
		Size	nbytes;
		void   *data;

		shm_mq_result	resmq;

		if (shutdown_requested)
			break;

		/* Wait to be signalled. */
		rc = WaitLatch(&worker_state->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
					   0, PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		/* Reset the latch so we don't spin. */
		ResetLatch(&worker_state->latch);

		if (shm_mq_get_sender(worker_state->mqin) == NULL)
		{
			CHECK_FOR_INTERRUPTS();
			continue;
		}

		if (!shm_mq_get_sender(worker_state->mqout))
			shm_mq_set_sender(worker_state->mqout, MyProc);

		if (!shm_mq_get_receiver(worker_state->mqin))
			shm_mq_set_receiver(worker_state->mqin, MyProc);

		mqh = shm_mq_attach(worker_state->mqin, NULL, NULL);
		resmq = shm_mq_receive(mqh, &nbytes, &data, false);

		if (resmq == SHM_MQ_DETACHED)
		{
			shm_mq_detach(mqh);
			continue;
		}

		if (resmq == SHM_MQ_SUCCESS)
		{
			JsonbcCommand	cmd;
			Oid				cmoptoid;
			shm_mq_iovec   *iov = NULL;
			char		   *ptr = data;
			int				nkeys = *((int *) ptr);
			size_t			iovlen;

			ptr += sizeof(int);
			cmoptoid = *((Oid *) ptr);
			ptr += sizeof(Oid);
			cmd = *((JsonbcCommand *) ptr);
			ptr += sizeof(JsonbcCommand);

			switch (cmd)
			{
				case JSONBD_CMD_GET_IDS:
					iov = (shm_mq_iovec *) palloc(sizeof(shm_mq_iovec));
					iovlen = 1;
					iov->data = jsonbd_cmd_get_ids(nkeys, cmoptoid, ptr, &iov->len);
					break;
				case JSONBD_CMD_GET_KEYS:
				{
					char **keys = jsonbd_cmd_get_keys(nkeys, cmoptoid, (uint32 *) ptr);
					if (keys != NULL)
					{
						int i;

						iov = (shm_mq_iovec *) palloc(sizeof(shm_mq_iovec) * nkeys);
						iovlen = nkeys;
						for (i = 0; i < nkeys; i++)
						{
							iov[i].data = keys[i];
							iov[i].len = strlen(keys[i]) + 1;
						}
					}

					break;
				}
				default:
					elog(NOTICE, "jsonbd: got unknown command");
			}

			shm_mq_detach(mqh);
			mqh = shm_mq_attach(worker_state->mqout, NULL, NULL);

			if (iov != NULL)
				resmq = shm_mq_sendv(mqh, iov, iovlen, false);
			else
				resmq = shm_mq_sendv(mqh, &((shm_mq_iovec) {"\0", 1}), 1, false);

			if (resmq != SHM_MQ_SUCCESS)
				elog(NOTICE, "jsonbd: backend detached early");

			shm_mq_detach(mqh);
			MemoryContextReset(worker_context);
			pg_atomic_clear_flag(&worker_state->busy);
		}
	}

finish:
	elog(LOG, "jsonbd dictionary worker has ended its work");
	proc_exit(0);
}

static bool
jsonbd_register_worker(int worker_num, Oid dboid, int database_num)
{
	BackgroundWorker		 worker;
	BackgroundWorkerHandle	*bgw_handle;
	jsonbd_worker_args		*worker_args;
	jsonbd_shm_hdr			*hdr;
	dsm_segment				*seg;

	if (worker_num > MAX_JSONBD_WORKERS)
	{
		elog(LOG, "Reached maximum count of jsonbd dictionary workers");
		return false;
	}

	/* Init launcher state */
	Assert(workers_data != NULL);
	hdr = shm_toc_lookup(shm_toc_attach(JSONBD_SHM_MQ_MAGIC, workers_data), 0, false);

	/* Initialize DSM segment */
	seg = dsm_create(sizeof(jsonbd_worker_args), 0);
	worker_args = (jsonbd_worker_args *) dsm_segment_address(seg);
	worker_args->worker_num = worker_num;
	worker_args->dboid = dboid;
	worker_args->database_num = database_num;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 0;
	worker.bgw_notify_pid = MyProcPid;
	memcpy(worker.bgw_library_name, "jsonbd", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(jsonbd_worker_main), BGW_MAXLEN);
	snprintf(worker.bgw_name, BGW_MAXLEN, "jsonbd, worker %d, db: %d",
			 worker_num, dboid);
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &bgw_handle))
	{
		elog(LOG, "jsonbd: could not start dictionary worker");
		return false;
	}

	/* Wait to be signalled. */
#if PG_VERSION_NUM >= 100000
	WaitLatch(&hdr->launcher_latch, WL_LATCH_SET, 0, PG_WAIT_EXTENSION);
#else
	WaitLatch(&hdr->launcher_latch, WL_LATCH_SET, 0);
#endif

	ResetLatch(&hdr->launcher_latch);

	/* Remove the segment */
	dsm_detach(seg);

	/* An interrupt may have occurred while we were waiting. */
	CHECK_FOR_INTERRUPTS();

	/* all good */
	return true;
}

void
jsonbd_register_launcher(void)
{
	BackgroundWorker worker;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 0;
	worker.bgw_notify_pid = 0;
	memcpy(worker.bgw_library_name, "jsonbd", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(jsonbd_launcher_main), BGW_MAXLEN);
	snprintf(worker.bgw_name, BGW_MAXLEN, "jsonbd launcher");
	worker.bgw_main_arg = (Datum) Int32GetDatum(0);
	RegisterBackgroundWorker(&worker);
}

Oid
jsonbd_get_dictionary_relid(void)
{
	Oid relid;

	if (OidIsValid(jsonbd_dictionary_reloid))
		return jsonbd_dictionary_reloid;

	start_xact_command();

	relid = get_relname_relid(JSONBD_DICTIONARY_REL, get_jsonbd_schema());
	if (relid == InvalidOid)
		elog(ERROR, "jsonbd dictionary relation does not exist");

	/* fill index Oids too */
	if (jsonbd_id_indoid == InvalidOid)
	{
		Relation	 rel;
		ListCell	*lc;
		List		*indexes;

		Assert(relid != InvalidOid);

		rel = relation_open(relid, NoLock);
		indexes = RelationGetIndexList(rel);
		Assert(list_length(indexes) == 2);

		foreach(lc, indexes)
		{
			Oid			indOid = lfirst_oid(lc);
			Relation	indRel = index_open(indOid, NoLock);
			int			attnum = indRel->rd_index->indkey.values[1];

			if (attnum == JSONBD_DICTIONARY_REL_ATT_ID)
				jsonbd_id_indoid = indOid;
			else
			{
				Assert(attnum == JSONBD_DICTIONARY_REL_ATT_KEY);
				jsonbd_keys_indoid = indOid;
			}

			index_close(indRel, NoLock);
		}
		relation_close(rel, NoLock);
	}

	finish_xact_command();

	/* check we did fill global variables */
	Assert(OidIsValid(jsonbd_id_indoid));
	Assert(OidIsValid(jsonbd_keys_indoid));
	Assert(OidIsValid(relid));

	jsonbd_dictionary_reloid = relid;
	return relid;
}

static char *
jsonbd_get_dictionary_name(Oid relid)
{
	HeapTuple	tp;
	Form_pg_class reltup;
	char	   *relname;
	char	   *nspname;
	char	   *result;
	MemoryContext	old_mcxt;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	reltup = (Form_pg_class) GETSTRUCT(tp);
	relname = NameStr(reltup->relname);

	nspname = get_namespace_name(reltup->relnamespace);
	if (!nspname)
		elog(ERROR, "cache lookup failed for namespace %u",
			 reltup->relnamespace);

	old_mcxt = MemoryContextSwitchTo(TopMemoryContext);
	result = quote_qualified_identifier(nspname, relname);
	MemoryContextSwitchTo(old_mcxt);

	ReleaseSysCache(tp);

	return result;
}
