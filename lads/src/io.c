#ifdef __linux__
#define _XOPEN_SOURCE (500)
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <inttypes.h>

#include "io.h"
#include "zs.h"
#include "master.h"

extern void release_file(zs_globals_t *zs, request_t *req);
extern void release_block(zs_globals_t *zs, request_t *req);
extern void write_block_from_ssd2(io_state_t *is, request_t *req);

static void
read_block_to_ssd(io_state_t *is, request_t *req, uint32_t index)
{
	int ret = 0, offset = 0;

	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_globals_t *zs = is->zs;

/*#ifdef DEBUG
	fprintf(stderr, "%s: ssd: %s\n", zs_role(is->zs), __func__);
#endif*/

	/* Read into buffer */
	if (zs->ssd_mmap_addr == NULL) {
/*#ifdef DEBUG
		fprintf(stderr, "ssd_mmap file does not exist\n");	
#endif*/
		goto out;
	}

	do {
//		ret = pread(b->file->src_fd,
//				(void *)zs->ssd_mmap_addr + zs->ssd_offset, 
//				b->len - offset, 
//				b->file_offset + offset);
		ret = pread(b->file->src_fd,
				(void *)zs->ssd_mmap_addr + (uint64_t) is->zs->rma_mtu * index, 
				b->len - offset, 
				b->file_offset + offset);

		if (ret > 0) {
			offset += ret;
		} else if (ret == 0) {
/*#ifdef DEBUG
			fprintf(stderr, "%s: reached EOF for %s unexpectedly "
					"at offset %"PRIu64"\n",
					__func__, b->file->name,
					b->file_offset + offset);
#endif*/
			assert(ret != 0);
		} else {
			if (errno == EINTR)
				continue;

/*#ifdef DEBUG
			fprintf(stderr, "%s: pread() for %s failed with %s\n",
					__func__, b->file->name,
					strerror(errno));
#endif*/
			assert(ret != -1);
		}

	} while (offset < b->len);

//	b->ssd_file_offset = zs->ssd_offset;
	b->ssd_file_offset = (uint64_t) is->zs->rma_mtu * index;
//	zs->ssd_offset += b->len;

	/* add into SSD queue */
	zs_ssd_t *ssd = zs->ssdq;

	pthread_mutex_lock(&ssd->lock);
	TAILQ_INSERT_TAIL(&ssd->q, req, entry);
	ssd->queued++;
	pthread_mutex_unlock(&ssd->lock);

    out:
	return;
}

static void
read_block(io_state_t *is, request_t *req)
{
	int ret = 0, unused = 0;
	uint32_t index = 0, offset = 0;
	uint32_t index_s = 0;
	zs_block_t *b = container_of(req, zs_block_t, req);
	char block[128];

/*#ifdef DEBUG
	fprintf(stderr, "%s: io: %s\n", zs_role(is->zs), __func__);
#endif*/

	zs_globals_t *zs = is->zs;

	if (zs->ssd_buffer_flag == 1) {

		/* Try to get RMA buffer */
		ret = try_ids_get(is->zs->ids, &index, NULL, b);

/*#ifdef DEBUG
		fprintf(stderr, "%s: no available RMA buffer\n\n", __func__); 
#endif*/

		if (ret == EAGAIN) {	// this ret means that there is no RMA slot available 
		
			/* Get SSD buffer - may block */
			ret = ids_get(is->zs->ids_ssd, &index_s, &is->zs->io[is->id]->wait, NULL);

/*#ifdef DEBUG
			fprintf(stderr, "%s: begin to read data block from disk into SSD buffer\n\n", __func__); 
#endif*/

			read_block_to_ssd(is, req, index_s);
			return;
		}
		else{
			goto next;
		}
	}

	/* Get RMA buffer - may block*/
	ret = ids_get(is->zs->ids, &index, &is->zs->io[is->id]->wait, b);

next:

/*#ifdef DEBUG
	fprintf(stderr, "******** DRAM buffer\n\n"); 
#endif*/

	b->src_offset = (uint64_t) index * (uint64_t) is->zs->rma_mtu;

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &unused);
/*#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: io %2d: %s: %s index %u\n",
				zs_role(is->zs), is->id, __func__, block, index);
#endif*/

	/* Read into buffer */
	do {
		ret = pread(b->file->src_fd,
				(void*)((uintptr_t)is->zs->rma_buf + b->src_offset + offset),
				b->len - offset, b->file_offset + offset);
		if (ret > 0) {
			offset += ret;
		} else if (ret == 0) {
/*#ifdef DEBUG
			fprintf(stderr, "%s: reached EOF for %s unexpectedly "
					"at offset %"PRIu64"\n",
					__func__, b->file->name,
					b->file_offset + offset);
#endif*/
			assert(ret != 0);
		} else {
			if (errno == EINTR)
				continue;

//#ifdef DEBUG
			fprintf(stderr, "%s: pread() for %s failed with %s\n",
					__func__, b->file->name,
					strerror(errno));

			fprintf(stderr, "b->src_offset: %lu, offset: %d\n", b->src_offset, offset);
			fprintf(stderr, "b->len: %d\n", b->len);
			fprintf(stderr, "offset: %lu, count: %d\n",  b->file_offset + offset, b->len - offset);
//#endif
			assert(ret != -1);
		}
	} while (offset < b->len);

/*#ifdef DEBUG
	fprintf(stderr, "%s: io: %s: block %p index %u src_offset %"PRIu64" - done\n",
			zs_role(is->zs), __func__, (void*)b, index, b->src_offset);
#endif*/

	/* queue on comm to send NEW_BLOCK msg */
	pthread_mutex_lock(&is->zs->comm[0]->lock);
	TAILQ_INSERT_TAIL(&is->zs->comm[0]->q, req, entry);
	pthread_mutex_unlock(&is->zs->comm[0]->lock);

	return;
}

static void
write_block_to_ssd(io_state_t *is, request_t *req, uint32_t index_s)
{
	int 		index, index2;
	void		*ptr;
	int 		ret = 0;
	int 		offset = 0;
	zs_block_t 	*b = container_of(req, zs_block_t, req);
	zs_globals_t 	*zs = is->zs;

/*#ifdef DEBUG
	fprintf(stderr, "%s: ssd: %s\n", zs_role(is->zs), __func__);
#endif*/

	/* Read into buffer */
	if (zs->ssd_mmap_addr == NULL) {
/*#ifdef DEBUG
		fprintf(stderr, "ssd_mmap file does not exist\n");	
#endif*/
		goto out;
	}

	/* Read into SSD buffer */
	ptr = memcpy ( (void*)((uintptr_t)zs->ssd_mmap_addr + (uintptr_t) (is->zs->rma_mtu * index_s)),
	               (void*)((uintptr_t)is->zs->rma_buf + b->sink_offset + offset), 
	               b->len);

	if (ptr == NULL) {
/*#ifdef DEBUG
		fprintf(stderr, "memcpy() failed\n");
#endif*/
		goto out;
	}

	/* Release RDMA block */
	index = (uint32_t) b->sink_offset / (uint64_t)is->zs->rma_mtu;
	ids_put(is->zs->ids, index, b);

	b->ssd_file_offset = (uint64_t) is->zs->rma_mtu * index_s;

	/* add into SSD queue */
	zs_ssd_t *ssd = zs->ssdq;

	pthread_mutex_lock(&ssd->lock);
	TAILQ_INSERT_TAIL(&ssd->q, req, entry);
	ssd->queued++;

	pthread_mutex_unlock(&ssd->lock);

	pthread_mutex_lock(zs->lock);
	if (ssd->queued == zs->ssd_cnt)	// SSD buffer is full
		zs->drain_flag = 1;
	pthread_mutex_unlock(zs->lock);

    out:
	return;

}

static void
write_block(io_state_t *is, request_t *req)
{
	int ret = 0, unused = 0;
	uint32_t offset = 0, index = 0;
	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_file_t *f = b->file;
	char block[128];

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &unused);
/*#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: io %2d: %s: %s index %u\n",
				zs_role(is->zs), is->id, __func__, block, index);
#endif*/

	uint32_t index_s = 0;
	zs_globals_t *zs = is->zs;
	if (zs->ssd_buffer_flag == 1) {

		if (1){	/* Check if this OST is overloaded and needs an I/O redirection to SSD 
			   Fix me laste; it's fixed "TRUE" always redirect to SSD */

			/* Get SSD buffer - may block */
			ret = ids_get(is->zs->ids_ssd, &index_s, &is->zs->io[is->id]->wait, NULL);

/*#ifdef DEBUG
			fprintf(stderr, "%s: begin to write data block from RMA buffer to SSD buffer\n\n", __func__);
#endif*/

			write_block_to_ssd(is, req, index_s);

/*#ifdef DEBUG
			fprintf(stderr, "###############################\n");
			fprintf(stderr, "SSD sink buffer being used\n");
			fprintf(stderr, "###############################\n");
#endif*/

			return;
		}
		else{
			goto next;
		}
	}

next:

	do {
		ret = pwrite(b->file->sink_fd,
				(void*)((uintptr_t)is->zs->rma_buf + b->sink_offset + offset),
				b->len - offset, b->file_offset + offset);
		if (ret >= 0) {
			offset += ret;
/*#ifdef DEBUG
			if (ret != b->len)
				fprintf(stderr, "%s: io %2d: %s: wrote %7d bytes\n",
						zs_role(is->zs), is->id, __func__, ret);
#endif*/
		} else {
			if (errno == EINTR)
				continue;

/*#ifdef DEBUG
			fprintf(stderr, "%s: io %2d: %s: pwrite() failed with %s "
					"rma_buf %p %s\n", zs_role(is->zs), is->id,
					__func__, strerror(errno), is->zs->rma_buf,
					block);
#endif*/
			assert(ret != -1);
		}
	} while (offset < b->len);

	index = (uint32_t) (b->sink_offset / (uint64_t)is->zs->rma_mtu);
	ids_put(is->zs->ids, index, b);

	release_block(is->zs, &b->req);

	pthread_mutex_lock(&f->lock);
	f->completed++;
	pthread_mutex_unlock(&f->lock);

	/* This is a stateful sink, so check if the file is complete */
	if (f->completed == f->num_blocks) {
		release_file(is->zs, &f->req);
	}

	return;
}

static void
do_io(io_state_t *is)
{
	zs_globals_t *zs = is->zs;
	request_t *req_drain = NULL;
	request_t *req = NULL;
	zs_ssd_t *ssd = NULL;

	while (!is->done && !zs->done) {

		zs_ost_t *ost = NULL;


		/* On sink, first check if all OST queues are empty. 
		 * If all OST queues are empty, then, check if SSD queue is empty. 
		 * If SSD queue is not empty, then, drain SSD buffer */
		if (is->zs->role == ZS_SINK)
		{
/*#ifdef DEBUG
			if (zs->ssd_buffer_flag == 1)
				fprintf(stderr, "ssd queued: %d\n", zs->ssdq->queued);
#endif*/

			zs_ost_t *o;
			int empty = 1; int i;

			for (i = 0; i < zs->num_osts; i++) {
				o = zs->osts[i];
				if (o->queued > 0) {
					empty = 0;
					break;
				}
			}
/*#ifdef DEBUG
			fprintf(stderr, "empty?: %d\n", empty);
#endif*/
			if (zs->ssd_buffer_flag == 1) {
				if (empty && (zs->ssdq->queued != 0 )) {
					goto ssd_drain;
				}
			}
		}

		/* will block if needed */
		master_get_ost(zs, &zs->io[is->id]->wait, &ost, is->id);

		if (is->done || zs->done)
			goto out;


		pthread_mutex_lock(&ost->lock);
		req = TAILQ_FIRST(&ost->q);
		if (req) {
			TAILQ_REMOVE(&ost->q, req, entry);
			ost->queued--;
		} else {
			ost->busy = 0;
		}
		pthread_mutex_unlock(&ost->lock);

		if (!req)
			continue;

		/* TODO read/write as needed */
		if (is->zs->role == ZS_SRC) {
			read_block(is, req);
		}
		else{
			write_block(is, req);

			/* check if sink drain flag is set */
			if ((zs->ssd_buffer_flag == 1) && (zs->drain_flag == 1)){
				goto ssd_drain;
			}
		}

		pthread_mutex_lock(&ost->lock);
		ost->busy = 0;
		pthread_mutex_unlock(&ost->lock);

ssd_drain:
		if (is->zs->role == ZS_SINK) {

			if (zs->ssd_buffer_flag == 1) {
				ssd = zs->ssdq;

				while(1) {

					pthread_mutex_lock(&ssd->lock);
					req_drain = TAILQ_FIRST(&ssd->q);

					if (req_drain) {
						TAILQ_REMOVE(&ssd->q, req_drain, entry);
						ssd->queued--;
					}
//					}else {
//						ssd->busy = 0;
//					}
					pthread_mutex_unlock(&ssd->lock);

					if (!req_drain) break;	// SSD buffer empty 

/*#ifdef DEBUG
					fprintf(stderr, "draining!!!\n\n"); 
#endif*/

					write_block_from_ssd2(is, req_drain);	// drain SSD buffer

//					pthread_mutex_lock(&ssd->lock);
//					ssd->busy = 0;
//					pthread_mutex_unlock(&ssd->lock);
//
					pthread_mutex_lock(zs->lock);
					if (ssd->queued == 0)	// SSD buffer is empty; all drained
						zs->drain_flag = 0;
					pthread_mutex_unlock(zs->lock);
				}
			}
		}
	}

    out:

#if 0

	// drain remaining items from SSD buffer
	if ( (is->zs->role == ZS_SINK) && (zs->ssd_buffer_flag == 1))
	{
		ssd = zs->ssdq;

		while(1){

			pthread_mutex_lock(&ssd->lock);
			req_drain = TAILQ_FIRST(&ssd->q);

			if (req_drain) {
				TAILQ_REMOVE(&ssd->q, req_drain, entry);
				ssd->queued--;
			}
//			}else {
//				ssd->busy = 0;
//			}
			pthread_mutex_unlock(&ssd->lock);

			if (!req_drain) break;	// SSD buffer empty 

			fprintf(stderr, "start draining!!!\n\n"); 
			write_block_from_ssd2(is, req_drain);	// drain SSD buffer

//			pthread_mutex_lock(&ssd->lock);
//			ssd->busy = 0;
//			pthread_mutex_unlock(&ssd->lock);
		}
	}
#endif

	return;
}

void
io(void *args)
{
	int i = 0;
	io_state_t *is = NULL;
	zs_globals_t *zs = (zs_globals_t*)args;
	pthread_t tid = pthread_self();

	is = calloc(1, sizeof(*is));
	if (!is) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: no memory for is\n", __func__);
#endif*/
		goto out;
	}
	is->zs = zs;

	/* determine who we are */
	is->id = -1;

	for (i = 0; i < zs->num_ios; i++) {
		if (pthread_equal(tid, zs->io[i]->tid))
			is->id = i;
	}

	assert(is->id != -1);

/*#ifdef DEBUG
	fprintf(stderr, "%s: %2d ready\n", __func__, is->id);
#endif*/

	/* wait for all threads to start */
	pthread_mutex_lock(zs->lock);
	if (!zs->done) {
		zs->ready++;
		waitq_wait_locked(&zs->start, &zs->io[is->id]->wait);
	}
	pthread_mutex_unlock(zs->lock);

	if (!zs->done)
		do_io(is);

    out:
/*#ifdef DEBUG
	fprintf(stderr, "%s: %2d shutting down\n", __func__, is->id);
#endif*/

	free(is);

	pthread_mutex_lock(zs->lock);
	zs->done++;
	pthread_mutex_unlock(zs->lock);

	pthread_exit(NULL);
}
