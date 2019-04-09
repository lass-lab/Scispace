#ifndef MASTER_H
#define MASTER_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "zs.h"
#include "waitq.h"

typedef struct master_state {
	zs_globals_t *zs;
	pthread_mutex_t ost_lock;
	int next_ost;
	int done;
} master_state_t;

int master_get_ost(zs_globals_t *zs, waiter_t *w, zs_ost_t **ost, int id);
int master_new_file(master_state_t *master, zs_file_t *f);
int master_file_id(master_state_t *master, zs_file_t *f);
int master_new_block(master_state_t *master, zs_file_t *f, zs_block_t *b);
int master_block_done(master_state_t *master, zs_block_t *b);
int master_file_close(master_state_t *master, zs_file_t *f);

#endif /* MASTER_H */
