#ifdef __linux__
#ifndef _GNU_SOUCE
#define _GNU_SOURCE
#endif
#endif
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "ids.h"

int
ids_init(uint32_t cnt, ids_t **idsp)
{
	int ret = 0, bits = 0;
	ids_t *b = NULL;

	if (!cnt || !idsp) {
		ret = EINVAL;
		goto out;
	}

	b = calloc(1, sizeof(*b));
	if (!b) {
		ret = ENOMEM;
		goto out;
	}

	b->cnt = cnt;
	b->avail = cnt;

	bits = sizeof(*b->blocks) * 8;
	assert(bits == 64);

	b->id_cnt = cnt / bits;
	if (cnt != (b->id_cnt * bits))
		b->id_cnt++;

	b->blocks = calloc(b->id_cnt, sizeof(*b->blocks));
	if (!b->blocks) {
		ret = ENOMEM;
		goto out;
	}
	/* Set all the bits now to indicate unused */
	memset(b->blocks, 0xFF, b->id_cnt * sizeof(*b->blocks));

	if (b->id_cnt * sizeof(*b->blocks) != b->cnt) {
		int i = b->cnt;
		uint64_t *block = &b->blocks[b->id_cnt - 1];

		for (; i < bits; i++)
			*block &= ~(((uint64_t) 1) << i);
	}

	b->contexts = calloc(cnt, sizeof(*b->contexts));
	if (!b->contexts) {
		ret = ENOMEM;
		goto out;
	}

	ret = pthread_mutex_init(&b->lock, NULL);
	if (ret)
		goto out;

	waitq_init(&b->q, &b->lock);

    out:
	if (ret == 0) {
		*idsp = b;
	} else {
		if (b) {
			free(b->contexts);
			free(b->blocks);
		}
		free(b);
	}
	return ret;
}

void
ids_destroy(ids_t *ids)
{
	if (ids) {
		waitq_broadcast(&ids->q);
		if (ids) {
			free(ids->contexts);
			free(ids->blocks);
		}
		free(ids);
	}

	return;
}

/* Non-blocking get while locked
 *
 * Returns 0 or EAGAIN if no blocks available
 */
static inline int
ids_try_get_locked(ids_t *ids, uint32_t *index, void *context)
{
	int ret = EAGAIN, i = 0, j = 0, idx = 0;

	if (!ids || !index) {
		ret = EINVAL;
		goto out;
	}

	if (ids->avail) {
		for (i = 0; i < ids->id_cnt; i++) {
			uint64_t *block = &ids->blocks[i];

			if (*block == 0)
				continue;

			j = ffsl(*block);
			j--;
			idx = i * 64 + j;
/*#ifdef DEBUG
			fprintf(stderr, "ids:   reserving index %d (i %d j %d) for context %p\n",
					idx, i, j, context);
#endif*/
			*block &= ~(((uint64_t) 1) << j);
			ids->avail--;
			*index = idx;
			ids->contexts[idx] = context;
			ret = 0;
			break;
		}
	}

    out:
	return ret;
}

/* Non-blocking get
 *
 * Returns 0 or EAGAIN if no blocks available
 */
inline int
ids_try_get(ids_t *ids, uint32_t *index, void *context)
{
	int ret = 0;

	pthread_mutex_lock(&ids->lock);
	ret = ids_try_get_locked(ids, index, context);
	pthread_mutex_unlock(&ids->lock);

	return ret;
}

/* Blocking get */
//youkim_begin
inline int
try_ids_get(ids_t *ids, uint32_t *index, waiter_t *w, void *context)
{
	int ret = 0;

	pthread_mutex_lock(&ids->lock);
	ret = ids_try_get_locked(ids, index, context);
	pthread_mutex_unlock(&ids->lock);

	return ret;
}
//youkim_end

inline int
ids_get(ids_t *ids, uint32_t *index, waiter_t *w, void *context)
{
	int ret = 0;

	pthread_mutex_lock(&ids->lock);
    again:
	ret = ids_try_get_locked(ids, index, context);
	if (ret == EAGAIN) {
/*#ifdef DEBUG
		fprintf(stderr, "ids: no ids - sleeping\n");
#endif*/
		waitq_wait_locked(&ids->q, w);
/*#ifdef DEBUG
		fprintf(stderr, "ids: waking\n");
#endif*/
		goto again;
	}
	pthread_mutex_unlock(&ids->lock);

	return ret;
}

inline int
ids_get_context(ids_t *ids, uint32_t index, void **context)
{
	int ret = 0;
	void *ctx = NULL;

	pthread_mutex_lock(&ids->lock);
	ctx = ids->contexts[index];
	pthread_mutex_unlock(&ids->lock);

	if (ctx) {
		*context = ctx;
	} else {
/*#ifdef DEBUG
		fprintf(stderr, "ids: %s: no context for index %u\n",
				__func__, index);
#endif*/
		ret = -1;
	}
	return ret;
}

inline void
ids_put(ids_t *ids, uint32_t index, void *context)
{
	int i = index / 64;
	int j = index - (i * 64);
	uint64_t *block = &ids->blocks[i];

/*#ifdef DEBUG
	fprintf(stderr, "ids: unreserving index %u for context %p\n", index, context);
#endif*/

	pthread_mutex_lock(&ids->lock);
	assert(context == ids->contexts[index]);
	ids->contexts[index] = NULL;
	*block |= (((uint64_t) 1) << j);
	ids->avail++;
	pthread_mutex_unlock(&ids->lock);
	waitq_signal(&ids->q);

	return;
}
