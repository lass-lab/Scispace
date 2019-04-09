#ifndef IO_H
#define IO_H

#include <stdio.h>
#include <pthread.h>
#include <sys/queue.h>

#include "zs.h"
#include "request.h"

typedef struct io_state {
	zs_globals_t *zs;
	int id;
	int done;
} io_state_t;

#endif /* IO_H */
