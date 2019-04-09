#ifndef SSD_H
#define SSD_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/mman.h>

#include "zs.h"
#include "request.h"

typedef struct ssd_stat {
	zs_globals_t *zs;
	int id;
	int done;
} ssd_stat_t;

#endif /* SSD_H */
