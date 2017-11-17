#ifndef JSONBC_UTILS_H
#define JSONBC_UTILS_H

#include "postgres.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"

extern RangeTblEntry * add_range_table_to_estate(EState *estate, Relation rel);
extern uint32 qhashmurmur3_32(const void *data, size_t nbytes);
extern void shm_mq_clean_receiver(shm_mq *mq);
extern void shm_mq_clean_sender(shm_mq *mq);

#endif
