#include "jsonbc.h"
#include "jsonbc_utils.h"

#include "postgres.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "utils/rel.h"

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

RangeTblEntry *
add_range_table_to_estate(EState *estate, Relation rel)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	char	   *refname = RelationGetRelationName(rel);

	rte->rtekind = RTE_RELATION;
	rte->alias = NULL;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;

	rte->eref = makeAlias(refname, NIL);

	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = false;

	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid;
	rte->selectedCols = NULL;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;

	estate->es_range_table = lappend(estate->es_range_table, rte);

	return rte;
}

/**
 * Get 32-bit Murmur3 hash. Ported from qLibc library.
 * Added compability with C99, and postgres code style
 *
 * @param data      source data
 * @param nbytes    size of data
 *
 * @return 32-bit unsigned hash value.
 *
 * @code
 *  uint32_t hashval = qhashmurmur3_32((void*)"hello", 5);
 * @endcode
 *
 * @code
 *  MurmurHash3 was created by Austin Appleby  in 2008. The initial
 *  implementation was published in C++ and placed in the public.
 *    https://sites.google.com/site/murmurhash/
 *  Seungyoung Kim has ported its implementation into C language
 *  in 2012 and published it as a part of qLibc component.
 * @endcode
 */
uint32 qhashmurmur3_32(const void *data, size_t nbytes)
{
    int		i,
			nblocks;
    uint32	k;
    uint32 *blocks;
    uint8  *tail;

    const uint32 c1 = 0xcc9e2d51;
    const uint32 c2 = 0x1b873593;

    uint32 h = 0;

	Assert(data != NULL && nbytes > 0);

    nblocks = nbytes / 4;
    blocks = (uint32 *) (data);
    tail = (uint8 *) ((char *) data + (nblocks * 4));

    for (i = 0; i < nblocks; i++)
	{
        k = blocks[i];

        k *= c1;
        k = (k << 15) | (k >> (32 - 15));
        k *= c2;

        h ^= k;
        h = (h << 13) | (h >> (32 - 13));
        h = (h * 5) + 0xe6546b64;
    }

    k = 0;
    switch (nbytes & 3)
	{
        case 3:
            k ^= tail[2] << 16;
        case 2:
            k ^= tail[1] << 8;
        case 1:
            k ^= tail[0];
            k *= c1;
            k = (k << 15) | (k >> (32 - 15));
            k *= c2;
            h ^= k;
    };

    h ^= nbytes;

    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

void
shm_mq_clean_sender(shm_mq *mq)
{
	struct shm_mq_alt	*amq = (struct shm_mq_alt *) mq;

	/* check that attributes are same and our struct still compatible with global shm_mq */
	Assert(shm_mq_get_sender(mq) == amq->mq_sender);
	Assert(shm_mq_get_receiver(mq) == amq->mq_receiver);

	amq->mq_sender = NULL;
	amq->mq_detached = false;
}

void
shm_mq_clean_receiver(shm_mq *mq)
{
	struct shm_mq_alt	*amq = (struct shm_mq_alt *) mq;

	/* check that attributes are same and our struct still compatible with global shm_mq */
	Assert(shm_mq_get_sender(mq) == amq->mq_sender);
	Assert(shm_mq_get_receiver(mq) == amq->mq_receiver);

	amq->mq_receiver = NULL;
	amq->mq_detached = false;
}
