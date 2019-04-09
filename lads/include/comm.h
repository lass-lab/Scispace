#ifndef COMM_H
#define COMM_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

#include "cci.h"
#include "zs.h"

typedef struct comm_state {
	zs_globals_t *zs;
	cci_endpoint_t *endpoint;
	cci_connection_t **connections;
	cci_rma_handle_t *local;
	cci_rma_handle_t remote;
	int id;
	int ready;				/* connected, start sending requests */
	int done;
	int attempt;
} comm_state_t;

#endif	/* COMM_H */

