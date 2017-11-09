#include "jsonbc.h"

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
#include "catalog/indexing.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

static bool xact_started = false;
static bool shutdown_requested = false;
static jsonbc_shm_worker	*worker_state;

Oid jsonbc_dictionary_reloid = InvalidOid;
Oid	jsonbc_keys_indoid = InvalidOid;
Oid	jsonbc_id_indoid = InvalidOid;

void worker_main(Datum arg);
static Oid jsonbc_get_dictionary_relid(void);
static Oid get_extension_schema(void);

#define JSONBC_DICTIONARY_REL	"jsonbc_dictionary"

static const char *sql_dictionary = "CREATE TABLE public." JSONBC_DICTIONARY_REL
		  " (cmopt OID NOT NULL,"
		  " id INT4	NOT NULL,"
		  " key TEXT NOT NULL);"
		  " CREATE UNIQUE INDEX jsonbc_dict_on_id ON " JSONBC_DICTIONARY_REL "(cmopt, id, key);"
		  " CREATE UNIQUE INDEX jsonbc_dict_on_key ON " JSONBC_DICTIONARY_REL " (cmopt, key);";

#define JSONBC_DICTIONARY_REL_ATT_CMOPT		1
#define JSONBC_DICTIONARY_REL_ATT_ID		2
#define JSONBC_DICTIONARY_REL_ATT_KEY		3

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

static void
init_local_variables(int worker_num)
{
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

/* Returns buffers with keys ordered by ids */
static char *
jsonbc_get_keys_slow(Oid cmoptoid, uint32 *ids, int nkeys, size_t *reslen)
{
	int				i;
	char		   *keys,
				   *keyptr;
	size_t			keyslen;

	Oid			relid = jsonbc_get_dictionary_relid();
	Relation	rel,
				idxrel;

	rel = relation_open(relid, AccessShareLock);
	idxrel = index_open(jsonbc_id_indoid, AccessShareLock);

	/* preallocate the memory, we give 10 bytes for each word at first */
	keyslen = nkeys * 10;
	keyptr = keys = (char *) palloc(keyslen);

	for (i = 0; i < nkeys; i++)
	{
		char		   *key;
		size_t			lenkey;
		IndexScanDesc	scan;
		ScanKeyData		skey[2];
		Datum			key_datum;
		HeapTuple		tup;
		bool			isNull;

		scan = index_beginscan(rel, idxrel, SnapshotAny, 2, 0);

		ScanKeyInit(&skey[0],
					JSONBC_DICTIONARY_REL_ATT_CMOPT,
					BTEqualStrategyNumber,
					F_OIDEQ,
					ObjectIdGetDatum(cmoptoid));
		ScanKeyInit(&skey[1],
					JSONBC_DICTIONARY_REL_ATT_ID,
					BTEqualStrategyNumber,
					F_INT4EQ,
					Int32GetDatum(ids[i]));

		tup = index_getnext(scan, ForwardScanDirection);
		if (tup == NULL)
			elog(ERROR, "key not found for cmopt=%d and id=%d", cmoptoid, ids[i]);

		key_datum = heap_getattr(tup, JSONBC_DICTIONARY_REL_ATT_KEY,
							  RelationGetDescr(rel), &isNull);
		Assert(isNull == false);

		key = DatumGetCString(key_datum);
		lenkey = strlen(key) + 1; /* include \0 */

		/* increase buffer if we need to */
		if ((keyptr - keys) + lenkey >= keyslen)
		{
			keyslen = keyslen * 2 + lenkey;
			keys = (char *) repalloc(keys, keyslen);
		}

		memcpy(keyptr, key, lenkey);
		keyptr += lenkey;

		index_endscan(scan);
	}

	index_close(idxrel, AccessShareLock);
	relation_close(rel, AccessShareLock);

	*reslen = keyptr - keys;
	return keys;
}

/*
 * Get key IDs using relation
 * TODO: change to direct access
 */
static void
jsonbc_get_key_ids_slow(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys)
{
	Relation	rel;

	int		i;
	Oid		relid = jsonbc_get_dictionary_relid();
	char   *nspc = get_namespace_name(get_extension_schema());

	rel = relation_open(relid, ShareLock);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	for (i = 0; i < nkeys; i++)
	{
		Datum		datum;
		bool		isnull;
		char	   *sql;

		sql = psprintf("SELECT id FROM %s.jsonbc_dictionary WHERE cmopt = %d"
					   "	AND key = '%s'", nspc, cmoptoid, buf);

		if (SPI_exec(sql, 0) != SPI_OK_SELECT)
			elog(ERROR, "SPI_exec failed");

		if (SPI_processed == 0)
		{
			char *sql2 = psprintf("with t as (select (coalesce(max(id), 0) + 1) new_id from "
						"%s.jsonbc_dictionary where cmopt = %d) insert into %s.jsonbc_dictionary"
						" select %d, t.new_id, '%s' from t returning id",
						nspc, cmoptoid, nspc, cmoptoid, buf);

			if (SPI_exec(sql2, 0) != SPI_OK_INSERT_RETURNING)
				elog(ERROR, "SPI_exec failed");
		}

		datum = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc,
							  1,
							  &isnull);
		if (isnull)
			elog(ERROR, "id is NULL");

		idsbuf[i] = DatumGetInt32(datum);

		/* move to next key */
		while (*buf != '\0')
			buf++;

		buf++;
		pfree(sql);
	}
	SPI_finish();
	relation_close(rel, ShareLock);
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
		jsonbc_get_key_ids_slow(cmoptoid, buf, idsbuf, nkeys);
		finish_xact_command();
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		elog(LOG, "jsonbc: error occured %s", error->message);
		FlushErrorState();
		pfree(error);

		idsbuf[0] = 0;
		*buflen = 1;
	}
	PG_END_TRY();

	return (char *) idsbuf;
}

static char *
jsonbc_cmd_get_keys(int nkeys, Oid cmoptoid, uint32 *ids, size_t *reslen)
{
	char		   *keys = NULL;
	MemoryContext	old_mcxt = CurrentMemoryContext;;

	PG_TRY();
	{
		start_xact_command();
		keys = jsonbc_get_keys_slow(cmoptoid, ids, nkeys, reslen);
		finish_xact_command();
	}
	PG_CATCH();
	{
		ErrorData  *error;
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		elog(LOG, "jsonbc: error occured %s", error->message);
		FlushErrorState();
		pfree(error);
	}
	PG_END_TRY();

	return keys;
}

void
worker_main(Datum arg)
{
	MemoryContext	worker_context;

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "jsonbc_worker");
	init_local_variables(DatumGetInt32(arg));

	worker_context = AllocSetContextCreate(TopMemoryContext,
										   "jsonbc worker context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(worker_context);

	while (true)
	{
		int		rc;
		Size	nbytes;
		void   *data;

		shm_mq_handle  *mqh;
		shm_mq_result	resmq;

		mqh = shm_mq_attach(worker_state->mqin, NULL, NULL);
		resmq = shm_mq_receive(mqh, &nbytes, &data, true);

		if (resmq == SHM_MQ_SUCCESS)
		{
			JsonbcCommand	cmd;
			Oid				cmoptoid;
			shm_mq_iovec	iov;
			char			*ptr = data;
			int				nkeys = *((int *) ptr);

			ptr += sizeof(int);
			cmoptoid = *((Oid *) ptr);
			ptr += sizeof(Oid);
			cmd = *((JsonbcCommand *) ptr);
			ptr += sizeof(JsonbcCommand);

			switch (cmd)
			{
				case JSONBC_CMD_GET_IDS:
					iov.data = jsonbc_cmd_get_ids(nkeys, cmoptoid, ptr, &iov.len);
					break;
				case JSONBC_CMD_GET_KEYS:
					iov.data = jsonbc_cmd_get_keys(nkeys, cmoptoid, (uint32 *) ptr, &iov.len);
					if (iov.data == NULL)
					{
						iov.data = "";
						iov.len = 1;
					}
				default:
					elog(NOTICE, "jsonbc: got unknown command");
			}

			shm_mq_detach(mqh);

			mqh = shm_mq_attach(worker_state->mqout, NULL, NULL);
			resmq = shm_mq_sendv(mqh, &iov, 1, false);
			if (resmq != SHM_MQ_SUCCESS)
				elog(NOTICE, "jsonbc: backend detached early");

			shm_mq_detach(mqh);
			pfree((void *) iov.data);
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
	Oid relid;

	if (OidIsValid(jsonbc_dictionary_reloid))
		return jsonbc_dictionary_reloid;

	relid = get_relname_relid(JSONBC_DICTIONARY_REL, InvalidOid);
	if (relid == InvalidOid)
	{
		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		if (SPI_exec(sql_dictionary, 0) != SPI_OK_UTILITY)
			elog(ERROR, "could not create \"jsonbc\" dictionary");

		SPI_finish();

		/* get just created table Oid */
		relid = get_relname_relid(JSONBC_DICTIONARY_REL, InvalidOid);
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

		/* FIXME: use smarter way to determine which is index is what */
		foreach(lc, indexes)
		{
			Oid			indOid = lfirst_oid(lc);
			Relation	indRel = index_open(indOid, NoLock);

			if (indRel->rd_index->indnatts == 2)
				jsonbc_id_indoid = indOid;
			else
				jsonbc_keys_indoid = indOid;

			index_close(indRel, NoLock);
		}
		relation_close(rel, NoLock);
	}

	/* check we did fill global variables */
	Assert(OidIsValid(jsonbc_id_indoid));
	Assert(OidIsValid(jsonbc_keys_indoid));
	Assert(OidIsValid(relid));

	jsonbc_dictionary_reloid = relid;
	return relid;
}

static Oid
get_extension_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_oid;

	if (!IsTransactionState())
		return InvalidOid;

	ext_oid = get_extension_oid("jsonbc", true);
	if (ext_oid == InvalidOid)
		return InvalidOid;

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_oid));

	rel = heap_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);
	return result;
}
