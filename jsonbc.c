#include "jsonbc.h"

#include "postgres.h"
#include "fmgr.h"

#include "access/compression.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "catalog/indexing.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_crc.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(jsonbc_compression_handler);
PG_FUNCTION_INFO_V1(int4_to_char);

/* we use one buffer for whole transaction to avoid extra allocations */
typedef struct
{
	char	   *buf;		/* keys */
	int			buflen;
	uint32	   *idsbuf;		/* key ids */
	int			idslen;

	MemoryContext	item_mcxt;
} CompressionThroughBuffers;

static MemoryContext compression_mcxt = NULL;
static CompressionThroughBuffers *compression_buffers = NULL;

static void init_memory_context(bool);
static void memory_reset_callback(void *arg);
static void get_key_ids(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys);
static char *get_key_by_id(Oid cmoptoid, int32 key_id);
static void encode_varbyte(uint32 val, unsigned char *ptr, int *len);
static uint32 decode_varbyte(unsigned char *ptr);
static char *packJsonbValue(JsonbValue *val, int header_size, int *len);
static Oid get_extension_schema(void);

/* TODO: change to worker, add caches and other stuff */
static void
get_key_ids(Oid cmoptoid, char *buf, uint32 *idsbuf, int nkeys)
{
	int		i;
	char   *nspc = get_namespace_name(get_extension_schema());

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
}

static char *
get_key_by_id(Oid cmoptoid, int32 key_id)
{
	MemoryContext	old_mcxt;

	char *nspc = get_namespace_name(get_extension_schema());
	char *res = NULL;
	char *sql = psprintf("SELECT key FROM %s.jsonbc_dictionary WHERE cmopt = %d"
				"	AND id = %d", nspc, cmoptoid, key_id);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (SPI_exec(sql, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed");

	if (SPI_processed > 0)
	{
		bool		isnull;
		Datum		datum;

		datum = SPI_getbinval(SPI_tuptable->vals[0],
							  SPI_tuptable->tupdesc,
							  1,
							  &isnull);
		if (isnull)
			elog(ERROR, "id is NULL");

		old_mcxt = MemoryContextSwitchTo(compression_mcxt);
		res = text_to_cstring(DatumGetTextP(datum));
		MemoryContextSwitchTo(old_mcxt);
	}
	else elog(ERROR, "key not found");

	pfree(sql);
	SPI_finish();

	return res;
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
			get_key_ids(ac->cmoptoid, buf, idsbuf, nkeys);

			/* replace the old keys with encoded ids */
			for (i = 0; i < nkeys; i++)
			{
				JsonbValue *v = &jbv->val.object.pairs[i].key;

				encode_varbyte(idsbuf[i], (unsigned char *) keyptr, &len);
				v->val.string.val = keyptr;
				v->val.string.len = len;
				keyptr += len;
			}
		}
	}

	/* don't compress scalar values */
	if (IsAJsonbScalar(jbv))
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
		if (r == WJB_KEY)
		{
			int32		key_id;

			Assert(v.type == jbvString);
			Assert(v.val.string.len <= 5);
			key_id = decode_varbyte((unsigned char *) v.val.string.val);

			v.type = jbvString;
			v.val.string.val = get_key_by_id(ac->cmoptoid, key_id);
			v.val.string.len = strlen(v.val.string.val);
		}
		jbv = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
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
