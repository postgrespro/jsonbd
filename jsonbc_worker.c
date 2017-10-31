#include "jsonbc.h"

#include "postgres.h"
#include "fmgr.h"
#include "pgstat.h"

#include "access/xact.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

static bool xact_started = false;
static bool shutdown_requested = false;
static jsonbc_shm_worker	*worker_state;
static shm_mq_handle		*worker_mq_handle_in;
static shm_mq_handle		*worker_mq_handle_out;

void worker_main(Datum arg);

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
	worker_mq_handle_in = shm_mq_attach(worker_state->mqin, NULL, NULL);

	/* output mq */
	shm_mq_set_sender(worker_state->mqout, MyProc);
	worker_mq_handle_out = shm_mq_attach(worker_state->mqout, NULL, NULL);

	/* not busy at start */
	pg_atomic_clear_flag(&worker_state->busy);

	elog(LOG, "jsonbc dictionary worker %d started", worker_num + 1);
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
										   "jsonbc context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(worker_context);

	while (true)
	{
		int		rc;
		Size	nbytes;
		void   *data;
		uint32 *idsbuf;

		shm_mq_result resmq = shm_mq_receive(worker_mq_handle_in, &nbytes,
											   &data, true);
		if (resmq == SHM_MQ_SUCCESS)
		{
			Oid				cmoptoid;
			shm_mq_iovec	iov;
			char			*ptr = data;
			int				nkeys = *((int *) ptr);
			bool			success = true;

			ptr += sizeof(int);
			cmoptoid = *((Oid *) ptr);
			ptr += sizeof(Oid);

			idsbuf = (uint32 *) palloc(sizeof(uint32) * nkeys);
			elog(LOG, "received data, %d keys, oid %d, bytes: %ld",
					nkeys, cmoptoid, nbytes - sizeof(int) - sizeof(Oid));

			PG_TRY();
			{
				start_xact_command();
				jsonbc_get_key_ids_slow(cmoptoid, ptr, idsbuf, nkeys);
				finish_xact_command();
			}
			PG_CATCH();
			{
				ErrorData  *error;
				MemoryContextSwitchTo(worker_context);
				error = CopyErrorData();
				elog(LOG, "jsonbc: error occured %s", error->message);
				FlushErrorState();
				pfree(error);

				success = false;
			}
			PG_END_TRY();

			if (success)
			{
				iov.data = (void *) idsbuf;
				iov.len = sizeof(uint32) * nkeys;
			}
			else
			{
				iov.data = "\0";
				iov.len = 1;
			}

			resmq = shm_mq_sendv(worker_mq_handle_out, &iov, 1, false);
			if (resmq != SHM_MQ_SUCCESS)
				elog(NOTICE, "jsonbc: backend detached early");

			pfree(idsbuf);
		}

		if (shutdown_requested)
			break;

		rc = WaitLatch(&MyProc->procLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH,
			0, PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		ResetLatch(&MyProc->procLatch);
	}

	shm_mq_detach(worker_mq_handle_in);
	shm_mq_detach(worker_mq_handle_out);
	elog(LOG, "jsonbc dictionary worker has ended its work");
	proc_exit(0);
}
