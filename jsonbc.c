#include "jsonbc.h"

#include "postgres.h"
#include "fmgr.h"

#include "access/compression.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/pg_crc.h"
#include "executor/spi.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(jsonbc_compression_handler);
PG_FUNCTION_INFO_V1(int4_to_char);

static MemoryContext compression_mcxt = NULL;

static uint32
get_key_id(Oid cmoptoid, char *key, int keylen)
{
	uint32		key_id = 0;
	char	   *s,
			   *sql;
	bool		isnull;
	Datum		datum;

	s = palloc(keylen + 1);
	memcpy(s, key, keylen);
	s[keylen] = '\0';

	sql = psprintf("SELECT id FROM jsonbc_dictionary WHERE cmopt = %d"
				"	AND key = '%s'", cmoptoid, s);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (SPI_exec(sql, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed");

	if (SPI_processed == 0)
	{
		char *sql2 = psprintf("with t as (select (coalesce(max(id), 0) + 1) new_id from "
					"jsonbc_dictionary where cmopt = %d) insert into jsonbc_dictionary"
					" select %d, t.new_id, '%s' from t returning id", cmoptoid, cmoptoid, s);

		if (SPI_exec(sql2, 0) != SPI_OK_INSERT_RETURNING)
			elog(ERROR, "SPI_exec failed");
	}

	datum = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc,
						  1,
						  &isnull);
	if (isnull)
		elog(ERROR, "id is NULL");

	key_id = DatumGetInt32(datum);

	pfree(s);
	pfree(sql);
	SPI_finish();

	return key_id;
}

static char *
get_key_by_id(Oid cmoptoid, int32 key_id)
{
	MemoryContext	old_mcxt;

	char *res = NULL;
	char *sql = psprintf("SELECT key FROM jsonbc_dictionary WHERE cmopt = %d"
				"	AND id = %d", cmoptoid, key_id);

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
	JsonbValue			v;
	JsonbIterator	   *it;
	JsonbValue		   *jbv = NULL;
	JsonbParseState	   *state = NULL;
	struct varlena	   *res;

	it = JsonbIteratorInit(&((Jsonb *) data)->root);
	while ((r = JsonbIteratorNext(&it, &v, false)) != 0)
	{
		if (r == WJB_KEY)
		{
			int					len;
			int32				key_id;
			unsigned char	   *ptr = palloc0(6);

			Assert(v.type == jbvString);
			key_id = get_key_id(ac->cmoptoid, v.val.string.val, v.val.string.len);

			encode_varbyte(key_id, ptr, &len);
			Assert(len <= 5);

			v.type = jbvString;
			v.val.string.val = (char *) ptr;
			v.val.string.len = len;
		}
		jbv = pushJsonbValue(&state, r, r < WJB_BEGIN_ARRAY ? &v : NULL);
	}

	/* don't compress scalar values */
	if (IsAJsonbScalar(jbv))
		return NULL;

	res = (struct varlena *) packJsonbValue(jbv, VARHDRSZ_CUSTOM_COMPRESSED, &size);
	SET_VARSIZE_COMPRESSED(res, size);
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

	Assert(compression_mcxt != NULL);
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
	MemoryContextReset(compression_mcxt);
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

	if (compression_mcxt == NULL)
		compression_mcxt = AllocSetContextCreate(TopTransactionContext,
												 "jsonbc compression context",
												 ALLOCSET_DEFAULT_SIZES);

	PG_RETURN_POINTER(cmr);
}
