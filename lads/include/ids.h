#ifndef IDS_H
#define IDS_H

#include <stdint.h>

#include "waitq.h"

typedef struct ids {
	pthread_mutex_t lock;
	waitq_t q;
	uint64_t *blocks;
	void **contexts;
	uint32_t id_cnt;
	uint32_t avail;
	uint32_t cnt;
} ids_t;

int ids_init(uint32_t cnt, ids_t **idsp);
void ids_destroy(ids_t *ids);
int ids_try_get(ids_t *ids, uint32_t *index, void *context);
int ids_get(ids_t *ids, uint32_t *index, waiter_t *w, void *context);
int ids_get_context(ids_t *ids, uint32_t index, void **context);
void ids_put(ids_t *ids, uint32_t index, void *context);
int try_ids_get(ids_t *ids, uint32_t *index, waiter_t *w, void *context);

#endif /* IDS_H */
