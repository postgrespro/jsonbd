#include "jsonbc.h"
#include "jsonbc_utils.h"

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_compression_opt.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

static bool						xact_started = false;
static bool						shutdown_requested = false;
static jsonbc_shm_worker	   *worker_state;
static MemoryContext			worker_context = NULL;
static MemoryContext			worker_cache_context = NULL;
static HTAB					   *cmcache;

Oid jsonbc_dictionary_reloid	= InvalidOid;
Oid	jsonbc_keys_indoid			= InvalidOid;
Oid	jsonbc_id_indoid			= InvalidOid;

void worker_main(Datum arg);
static Oid jsonbc_get_dictionary_relid(void);

#define JSONBC_DICTIONARY_REL	"jsonbc_dictionary"

static const char *sql_dictionary = \
	"CREATE TABLE public." JSONBC_DICTIONARY_REL
	" (cmopt	OID NOT NULL,"
	"  id		INT4 NOT NULL,"
	"  key		TEXT NOT NULL);"
	"CREATE UNIQUE INDEX jsonbc_dict_on_id ON " JSONBC_DICTIONARY_REL "(cmopt, id);"
	"CREATE UNIQUE INDEX jsonbc_dict_on_key ON " JSONBC_DICTIONARY_REL " (cmopt, key);";

static const char *sql_insert = \
	"WITH t AS (SELECT (COALESCE(MAX(id), 0) + 1) new_id FROM "
	JSONBC_DICTIONARY_REL " WHERE cmopt = %d) INSERT INTO " JSONBC_DICTIONARY_REL
	" SELECT %d, t.new_id, '%s' FROM t RETURNING id";

enum {
	JSONBC_DICTIONARY_REL_ATT_CMOPT = 1,
	JSONBC_DICTIONARY_REL_ATT_ID,
	JSONBC_DICTIONARY_REL_ATT_KEY,
	JSONBC_DICTIONARY_REL_ATT_COUNT
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
static jsonbc_cached_cmopt *
get_cached_compression_options(Oid cmoptoid)
{
	bool	found;
	jsonbc_cached_cmopt *cmdata;

	cmdata = hash_search(cmcache, &cmoptoid, HASH_ENTER, &found);
	if (!found)
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(uint32);
		hash_ctl.entrysize = sizeof(jsonbc_cached_key);
		hash_ctl.hcxt = worker_cache_context;

		cmdata->cmoptoid = cmoptoid;
		cmdata->key_cache = hash_create("jsonbc map by key",
							  128,
							  &hash_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		hash_ctl.entrysize = sizeof(jsonbc_cached_id);
		cmdata->id_cache = hash_create("jsonbc map by id",
							  128,
							  &hash_ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return cmdata;
}

static void
init_local_variables(int worker_num)
{
	HASHCTL		hash_ctl;

	shm_toc		   *toc = shm_toc_attach(JSONBC_SHM_MQ_MAGIC, workers_data);
	jsonbc_shm_hdr *hdr = shm_toc_lookup(toc, 0, false);
	hdr->workers_ready++;

	worker_state = shm_toc_lookup(toc, worker_num + 1, false);
	worker_state->proc = MyProc;

	/* input mq */
	shm_mq_set_receiver(worker_state->mqin, MyProc);

	/* output mq */
	shm_mq_set_sender(worker_state->mqout, MyProc);

	/* not busy at start */
	pg_atomic_clear_flag(&worker_state->busy);

	/* this context will be reset after each task */
	Assert(worker_context == NULL);
	worker_context = AllocSetContextCreate(TopMemoryContext,
										   "jsonbc worker context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);

	worker_cache_context = AllocSetContextCreate(TopMemoryContext,
										"jsonbc worker cache context",
										ALLOCSET_DEFAULT_SIZES);

	/* Initialize hash tables used to track update chains */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(jsonbc_cached_cmopt);
	hash_ctl.hcxt = worker_cache_context;

	cmcache = hash_create("jsonbc compression options cache",
						  128,		/* arbitrary initial size */
						  &hash_ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	elog(LOG, "jsonbc dictionary worker %d started with pid: %d",
			worker_num + 1, MyProcPid);
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

void
jsonbc_register_worker(int n)
{
	BackgroundWorker worker;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
					   BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 0;
	worker.bgw_notify_pid = 0;
	memcpy(worker.bgw_library_name, "jsonbc", BGW_MAXLEN);
	memcpy(worker.bgw_function_name, CppAsString(worker_main), BGW_MAXLEN);
	snprintf(worker.bgw_name, BGW_MAXLEN, "jsonbc dictionary worker %d", n + 1);
	worker.bgw_main_arg = (Datum) Int32GetDatum(n);
	RegisterBackgroundWorker(&worker);
}

static char *
jsonbc_get_key(Relation rel, Relation indrel, Oid cmoptoid, uint32 key_id)
{
	IndexScanDesc	scan;
	ScanKeyData		skey[2];
	Datum			key_datum;
	HeapTuple		tup;
	bool			isNull;
	MemoryContext	old_mcxt;
	char		   *res;

	ScanKeyInit(&skey[0],
				JSONBC_DICTIONARY_REL_ATT_CMOPT,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(cmoptoid));
	ScanKeyInit(&skey[1],
				JSONBC_DICTIONARY_REL_ATT_ID,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(key_id));

	scan = index_beginscan(rel, indrel, SnapshotAny, 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);

	tup = index_getnext(scan, ForwardScanDirection);
	if (tup == NULL)
		elog(ERROR, "key not found for cmopt=%d and id=%d", cmoptoid, key_id);

	key_datum = heap_getattr(tup, JSONBC_DICTIONARY_REL_ATT_KEY,
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
jsonbc_get_keys(Oid cmoptoid, uint32 *ids, int nkeys)
{
	int				i;
	char		  **keys;
	jsonbc_cached_cmopt		*cmcache;

	Oid			relid = jsonbc_get_dictionary_relid();
	Relation	rel = NULL,
				indrel;

	cmcache = get_cached_compression_options(cmoptoid);
	keys = (char **) MemoryContextAlloc(worker_context,
										sizeof(char *) * nkeys);

	for (i = 0; i < nkeys; i++)
	{
		bool		found;
		MemoryContext	oldcontext;
		jsonbc_cached_id		*cid;
		jsonbc_pair				*pair;

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
			indrel = index_open(jsonbc_id_indoid, AccessShareLock);
		}
		keys[i] = jsonbc_get_key(rel, indrel, cmoptoid, ids[i]);

		/* create new pair and save it in cache */
		oldcontext = MemoryContextSwitchTo(worker_cache_context);
		pair = (jsonbc_pair *) palloc(sizeof(jsonbc_pair));
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
jsonbc_get_key_id(Relation rel, Relation indrel, Oid cmoptoid, char *key)
{
	IndexScanDesc	scan;
	ScanKeyData		skey[2];
	HeapTuple		tup;
	bool			isNull;
	uint32			result = 0;

	ScanKeyInit(&skey[0],
				JSONBC_DICTIONARY_REL_ATT_CMOPT,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(cmoptoid));
	ScanKeyInit(&skey[1],
				JSONBC_DICTIONARY_REL_ATT_KEY,
				BTEqualStrategyNumber,
				F_TEXTEQ,
				CStringGetTextDatum(key));

	scan = index_beginscan(rel, indrel, SnapshotAny, 2, 0);
	index_rescan(scan, skey, 2, NULL, 0);

	tup = index_getnext(scan, ForwardScanDirection);
	if (tup != NULL)
	{
		Datum dat = heap_getattr(tup, JSONBC_DICTIONARY_REL_ATT_ID,
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
static void
jsonbc_get_key_ids(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys)
{
	Relation	rel = NULL;
	int			i;
	Oid			relid = jsonbc_get_dictionary_relid();
	bool		spi_on = false;
	jsonbc_cached_cmopt		*cmcache;

	cmcache = get_cached_compression_options(cmoptoid);

	for (i = 0; i < nkeys; i++)
	{
		uint32		hkey;
		bool		found;
		Relation	indrel;
		MemoryContext	oldcontext;
		jsonbc_cached_key		*ckey;
		jsonbc_pair				*pair;

		hkey = qhashmurmur3_32(buf, strlen(buf));

		Assert(cmcache->key_cache);
		ckey = hash_search(cmcache->key_cache, &hkey, HASH_ENTER, &found);
		if (found)
		{
			ListCell	*lc;

			/* collisions check */
			foreach(lc, ckey->pairs)
			{
				jsonbc_pair	*pair = lfirst(lc);
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
		pair = (jsonbc_pair *) palloc(sizeof(jsonbc_pair));
		pair->key = pstrdup(buf);
		ckey->pairs = lappend(ckey->pairs, pair);
		MemoryContextSwitchTo(oldcontext);

		/* lazy transaction creation */
		if (!rel)
		{
			start_xact_command();
			rel = relation_open(relid, AccessShareLock);
		}

		indrel = index_open(jsonbc_keys_indoid, AccessShareLock);
		idsbuf[i] = jsonbc_get_key_id(rel, indrel, cmoptoid, buf);

		if (idsbuf[i] == 0)
		{
			Relation	indrel2;

			indrel2 = index_open(jsonbc_keys_indoid, ExclusiveLock);

			/* recheck, key could be added while we wait for lock */
			idsbuf[i] = jsonbc_get_key_id(rel, indrel2, cmoptoid, buf);

			if (idsbuf[i] == 0)
			{
				/* still need to add */
				Datum	datum;
				bool	isnull;
				char   *sql2 = psprintf(sql_insert, cmoptoid, cmoptoid, buf);

				if (!spi_on)
				{
					if (SPI_connect() != SPI_OK_CONNECT)
						elog(ERROR, "SPI_connect failed");

					spi_on = true;
				}

				if (SPI_exec(sql2, 0) != SPI_OK_INSERT_RETURNING)
					elog(ERROR, "SPI_exec failed");

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
		index_close(indrel, AccessShareLock);

		pair->id = idsbuf[i];
next:
		Assert(idsbuf[i] > 0);

		/* move to next key */
		while (*buf != '\0')
			buf++;

		buf++;
	}

	if (spi_on)
		SPI_finish();

	if (rel)
	{
		relation_close(rel, AccessShareLock);
		finish_xact_command();
	}
}

static char *
jsonbc_cmd_get_ids(int nkeys, Oid cmoptoid, char *buf, size_t *buflen)
{
	uint32		   *idsbuf;
	MemoryContext	old_mcxt = CurrentMemoryContext;;

	*buflen = nkeys * sizeof(uint32);
	idsbuf = (uint32 *) palloc(*buflen);

	PG_TRY();
	{
		start_xact_command();
		jsonbc_get_key_ids(cmoptoid, buf, idsbuf, nkeys);
		finish_xact_command();
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		elog(LOG, "jsonbc: error occured: %s", error->message);
		FlushErrorState();
		pfree(error);

		idsbuf[0] = 0;
		*buflen = 1;
	}
	PG_END_TRY();

	return (char *) idsbuf;
}

static char **
jsonbc_cmd_get_keys(int nkeys, Oid cmoptoid, uint32 *ids)
{
	char		  **keys = NULL;
	MemoryContext	mcxt = CurrentMemoryContext;;

	PG_TRY();
	{
		keys = jsonbc_get_keys(cmoptoid, ids, nkeys);
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(mcxt);
		error = CopyErrorData();
		elog(LOG, "jsonbc: error occured: %s", error->message);
		FlushErrorState();
		pfree(error);
	}
	PG_END_TRY();

	return keys;
}

void
worker_main(Datum arg)
{
	shm_mq_handle  *mqh = NULL;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbc_worker");
	init_local_variables(DatumGetInt32(arg));

	MemoryContextSwitchTo(worker_context);

	while (true)
	{
		int		rc;
		Size	nbytes;
		void   *data;

		shm_mq_result	resmq;

		if (!mqh)
			mqh = shm_mq_attach(worker_state->mqin, NULL, NULL);

		resmq = shm_mq_receive(mqh, &nbytes, &data, true);

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
				case JSONBC_CMD_GET_IDS:
					iov = (shm_mq_iovec *) palloc(sizeof(shm_mq_iovec));
					iovlen = 1;
					iov->data = jsonbc_cmd_get_ids(nkeys, cmoptoid, ptr, &iov->len);
					break;
				case JSONBC_CMD_GET_KEYS:
				{
					char **keys = jsonbc_cmd_get_keys(nkeys, cmoptoid, (uint32 *) ptr);
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
					elog(NOTICE, "jsonbc: got unknown command");
			}

			shm_mq_detach(mqh);
			mqh = shm_mq_attach(worker_state->mqout, NULL, NULL);

			if (iov != NULL)
				resmq = shm_mq_sendv(mqh, iov, iovlen, false);
			else
				resmq = shm_mq_sendv(mqh, &((shm_mq_iovec) {"\0", 1}), 1, false);

			if (resmq != SHM_MQ_SUCCESS)
				elog(NOTICE, "jsonbc: backend detached early");

			shm_mq_detach(mqh);
			MemoryContextReset(worker_context);

			/* mark we need new handle */
			mqh = NULL;
		}

		if (shutdown_requested)
			break;

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
			0, PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		ResetLatch(&MyProc->procLatch);
	}

	elog(LOG, "jsonbc dictionary worker has ended its work");
	proc_exit(0);
}

static Oid
jsonbc_get_dictionary_relid(void)
{
	Oid relid,
		nspoid;

	if (OidIsValid(jsonbc_dictionary_reloid))
		return jsonbc_dictionary_reloid;

	start_xact_command();

	nspoid = get_namespace_oid("public", false);
	relid = get_relname_relid(JSONBC_DICTIONARY_REL, nspoid);
	if (relid == InvalidOid)
	{
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		if (SPI_execute(sql_dictionary, false, 0) != SPI_OK_UTILITY)
			elog(ERROR, "could not create \"jsonbc\" dictionary");

		SPI_finish();
		CommandCounterIncrement();

		finish_xact_command();
		start_xact_command();

		/* get just created table Oid */
		relid = get_relname_relid(JSONBC_DICTIONARY_REL, nspoid);
		jsonbc_id_indoid = InvalidOid;
		jsonbc_keys_indoid = InvalidOid;
	}

	/* fill index Oids too */
	if (jsonbc_id_indoid == InvalidOid)
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

			if (attnum == JSONBC_DICTIONARY_REL_ATT_ID)
				jsonbc_id_indoid = indOid;
			else
			{
				Assert(attnum == JSONBC_DICTIONARY_REL_ATT_KEY);
				jsonbc_keys_indoid = indOid;
			}

			index_close(indRel, NoLock);
		}
		relation_close(rel, NoLock);
	}

	finish_xact_command();

	/* check we did fill global variables */
	Assert(OidIsValid(jsonbc_id_indoid));
	Assert(OidIsValid(jsonbc_keys_indoid));
	Assert(OidIsValid(relid));

	jsonbc_dictionary_reloid = relid;
	return relid;
}
