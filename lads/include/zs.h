#ifndef ZS_H
#define ZS_H

#include <pthread.h>
#include <stdint.h>
#include <sys/queue.h>
#include <stddef.h>

#include "fd_tree.h"
#include "request.h"
#include "waitq.h"
#include "ids.h"

#define DEBUG 0

#if DEBUG
#define ZS_DEBUG(...) do {        \
	fprintf(stderr, __VA_ARGS__);	\
  } while (0)
#else
#define ZS_DEBUG(fmt,...) do { } while (0)
#endif

#define ZS_RMA_MTU	(1024*1024)	/* default RMA blocksize */
#define ZS_RMA_BLOCKS	(256)		/* default number of RMA blocks */
#define ZS_SRC_DIR	"./src/"	/* default src dir name */
#define ZS_SINK_DIR	"./sink/"	/* default sink dir name */
#define ZS_MAX_NUM_OSTS	1024		/* default maximum number of OSTs */
#define ZS_SRC_SSD_LOG	"./src_ssd/log" 	/* default ssd dir name for source */
#define ZS_SINK_SSD_LOG	"./sink_ssd/log" 	/* default ssd dir name for sink */

#define ZS_NUM_COMM	(1)		/* default number of comm threads */
#define ZS_NUM_CONN	(1)		/* default number of connections */
#define ZS_NUM_IO	(4)		/* default number of io threads */
#define ZS_NUM_SSD	(1)		/* default number of ssd threads */
#define ZS_NUM_OSTS	(12)		/* default number of OST queues */
//#define ZS_NUM_OSTS	(8192)		/* default number of OST queues */
//#define ZS_NUM_OSTS	(50000)		/* default number of OST queues */

//#define MB 1024*1024
//#define GB 1024*MB
//#define SSD_BUFFER_SIZE 100*MB
#define ZS_SSD_BLOCKS (256)	/* default number of SSD buffer blocks */

typedef struct zs_globals zs_globals_t;
typedef struct zs_ost     zs_ost_t;
typedef struct zs_file    zs_file_t;
typedef struct zs_block   zs_block_t;

typedef struct zs_ssd     zs_ssd_t;

struct zs_globals {
	struct master_thread {
		pthread_t tid;
		TAILQ_HEAD(rq, request) q; /* queue for new requests */
		pthread_mutex_t lock;	/* protects q, m_wq, and ios_wq */
		waitq_t m_wq;		/* wait queue for master thread */
		waitq_t ios_wq;		/* wait queue for io threads */
		waitq_t ssd_wq;		/* wait queue for ssd thread */
		waiter_t wait;		/* sleep on zs->start at startup */
		void *priv;		/* pointer to master_state_t */
	} master;

	struct comm_thread {
		pthread_t tid;
		TAILQ_HEAD(cq, request) q; /* queue for new requests */
		pthread_mutex_t lock;	/* protects comm->q */
		void *priv;		/* pointer to comm_state_t */
	} **comm;

	struct io_thread {
		pthread_t tid;
		waiter_t wait;		/* sleep on master->ios_wq and zs->start */
		void *priv;		/* pointer to io_state_t */
	} **io;

	struct ssd_thread{
		pthread_t tid;
		waiter_t wait;
		void *priv;
	} **ssd;

	zs_ost_t **osts;		/* Array of pointers to ost queues */

	zs_ssd_t *ssdq;			/* a pointer to SSD queue */

	waitq_t start;			/* Master and IO threads at start only */
	ids_t *ids;			/* Manages buffer slot availability */
	ids_t *ids_ssd; 		/* Manages SSD buffer slot availability */
	void *rma_buf;			/* Pointer to RMA buffer */
	int done;			/* Each thread increments when exiting */
	int comm_done;			/* Each thread increments when exiting */
	uint32_t rma_mtu;		/* RMA blocksize */
	uint32_t rma_cnt;		/* Number of RMA blocks */
	uint32_t ssd_cnt; 		/* Number of SSD blocks */
	int num_comms;			/* Number of comm threads */
	int num_conns;			/* Number of connections */
	int num_ios;			/* Number of IO threads */
	int num_ssds;			/* Number of SSD threads */
	int num_osts;			/* Number of OST queues */
	int num_files;			/* Total number of files */
	int files_completed;		/* Number of completed files */
	int ready;			/* Is == (num_comms + num_ios)? */
	pthread_mutex_t *lock;		/* Protects waiters, ready and done */

	fd_tree_t *ftree;		/* Binary tree of FD and lock */

#define ZS_SRC	(1)
#define ZS_SINK	(2)
	int role;

	char *server_uri;		/* Only set if ZS_SRC */
	char *dir_name;			/* Directory for files */
	char *ssd_log_file_name; 	/* Directory for SSD buffer */
	int ssd_fd;		

	void *ssd_mmap_addr; 		/* memory address to a file mapped in SSD  */
	uint64_t ssd_offset;
	int ssd_buffer_flag;		/* flag whether to use the extended buffer on SSD */
	int drain_flag;			/* flag whether to drain SSD buffer or not */
};

static inline char *
zs_role(zs_globals_t *zs)
{
	if (zs->role == ZS_SRC)
		return "ZS_SRC";
	return "ZS_SINK";
}

struct zs_ost {
	TAILQ_HEAD(oq, request) q;	/* queue for new requests */
	pthread_mutex_t lock;		/* to protect ost */
	uint32_t queued;		/* number of requests queued */
	uint32_t busy;			/* is reading/writing? */
};

struct zs_ssd {
	TAILQ_HEAD(sq, request) q;	
	pthread_mutex_t lock;
	uint32_t queued; 
	uint32_t busy;
};

struct zs_file {
	request_t req;			/* type and entry */
	char *name;			/* File name */
	uint64_t len;			/* File length */
	pthread_mutex_t lock;		/* protects pending and completed */
	zs_ost_t **osts;		/* Array of OST pointers */
	uint32_t ost_offset;		/* Start offset */   
	uint32_t ost_cnt;		/* Number of OSTs */
	uint32_t ost_mtu;		/* Size of object in OST */
	int src_fd;			/* The source's file descriptor */
	int sink_fd;			/* The sink's file descriptor */
	uint32_t num_blocks;		/* Total blocks in file */
	uint32_t pending;		/* Blocks queued or sent (i.e. next block) */
	uint32_t completed;		/* Blocks transfered */
};

struct zs_block {
	request_t req;			/* type and entry */
	zs_file_t *file;		/* Pointer to owning file */
	uint64_t file_offset;		/* Offset in file */
	uint64_t src_offset;		/* Offset in SRC's RMA buffer */
	uint64_t sink_offset;		/* Offset in SINK's RMA buffer */
	zs_ost_t *ost;			/* Pointer to OST for this block */
	uint32_t len;			/* Length of block */
	uint64_t ssd_file_offset;	/* Offset in file in SSD */
};

int sprint_block(zs_block_t *b, char *buf, int buf_len, int *offset);
int print_block(zs_block_t *b);

/*! Obtain the private struct from the public struct
 *  Example 1:
 *    request_t *r;
 *    zs_file_t *file;
 *
 *    file = container_of(r, zs_file_t, req);
 *
 *  Example 2:
 *    request *req;
 *    zs_block_t *block;
 *
 *    block = container_of(req, zs_block_t, req);
 *
 *    where the first use of "req" is the variable
 *    the "zs_block_t" is the parent struct
 *    and the second req is the name of the field in the parent struct
 *
 *    If we always use the name of the field in the parent struct for
 *    the local variable name, then the name is repeated as in
 *    example 2 */
#define container_of(p,stype,field) ((stype *)(((uint8_t *)(p)) - offsetof(stype, field)))

#endif /* ZS_H */
