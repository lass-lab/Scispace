#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "ssd.h"
#include "io.h"

void write_block_from_ssd2(io_state_t *is, request_t *req)
{
	int ret = 0, unused = 0; 
	uint32_t index = 0, offset = 0;
	uint32_t index_s = 0;

	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_file_t *f = b->file;

	zs_globals_t *zs = is->zs;
	char block[128];

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &unused);

#ifdef DEBUG
	if (!ret)
                fprintf(stderr, "%s: io %2d: %s: %s index %u\n",
			                                zs_role(is->zs), is->id, __func__, block, index);
#endif

	do {
		ret  = pwrite(b->file->sink_fd, 
				(void *)((uintptr_t)zs->ssd_mmap_addr + b->ssd_file_offset + offset), 
				b->len - offset, b->file_offset + offset);
		if (ret >= 0) {
			offset += ret;
#ifdef DEBUG
			if (ret != b->len)
				fprintf(stderr, "%s: io %2d: %s: wrote %7d bytes\n",
				zs_role(is->zs), is->id, __func__, ret);
#endif
			} else {
				if (errno == EINTR)
					continue;
#ifdef DEBUG
				fprintf(stderr, "%s: io %2d: %s: pwrite() failed with %s "
				"rma_buf %p %s\n", zs_role(is->zs), is->id,
				__func__, strerror(errno), is->zs->rma_buf,
				block);
#endif
				assert(ret != -1);
		}
	} while (offset < b->len);


	index_s = (uint32_t) (b->ssd_file_offset / (uint64_t)is->zs->rma_mtu);
	ids_put(is->zs->ids_ssd, index_s, NULL);

	release_block(is->zs, &b->req);

	pthread_mutex_lock(&f->lock);
	f->completed++;
	pthread_mutex_unlock(&f->lock);

	/* This is a stateful
		* sink, so check if
		* the file is complete
	* */
	if (f->completed == f->num_blocks) {
		release_file(is->zs, &f->req);
	}

	return;
}

static void write_block_from_ssd(ssd_stat_t *ss, request_t *req)
{
	int ret = 0, unused = 0; 
	uint32_t index = 0, offset = 0;
	uint32_t index_s = 0;

	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_file_t *f = b->file;

	zs_globals_t *zs = ss->zs;
	char block[128];

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &unused);

#ifdef DEBUG
	if (!ret)
                fprintf(stderr, "%s: io %2d: %s: %s index %u\n",
			                                zs_role(ss->zs), ss->id, __func__, block, index);
#endif

	do {
		ret  = pwrite(b->file->sink_fd, 
				(void *)((uintptr_t)zs->ssd_mmap_addr + b->ssd_file_offset + offset), 
				b->len - offset, b->file_offset + offset);
		if (ret >= 0) {
			offset += ret;
#ifdef DEBUG
			if (ret != b->len)
				fprintf(stderr, "%s: io %2d: %s: wrote %7d bytes\n",
				zs_role(ss->zs), ss->id, __func__, ret);
#endif
			} else {
				if (errno == EINTR)
					continue;
#ifdef DEBUG
				fprintf(stderr, "%s: io %2d: %s: pwrite() failed with %s "
				"rma_buf %p %s\n", zs_role(ss->zs), ss->id,
				__func__, strerror(errno), ss->zs->rma_buf,
				block);
#endif
				assert(ret != -1);
		}
	} while (offset < b->len);


	index_s = (uint32_t) (b->ssd_file_offset / (uint64_t)ss->zs->rma_mtu);
	ids_put(ss->zs->ids_ssd, index_s, NULL);

	release_block(ss->zs, &b->req);

	pthread_mutex_lock(&f->lock);
	f->completed++;
	pthread_mutex_unlock(&f->lock);

	/* This is a stateful
		* sink, so check if
		* the file is complete
	* */
	if (f->completed == f->num_blocks) {
		release_file(ss->zs, &f->req);
	}

	return;
}

static void read_block_from_ssd(ssd_stat_t *ss, request_t *req)
{

#ifdef DEBUG
	fprintf(stderr, "\n\nSSD buffer used\n\n");  
#endif

	int ret = 0, unused = 0; 
	uint32_t index = 0, offset = 0;
	uint32_t index_s = 0;
	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_globals_t *zs = ss->zs;
	char block[128];

#ifdef DEBUG
	fprintf(stderr, "%s: ssd: %s\n", zs_role(ss->zs), __func__);
#endif
	
	/* Get RMA buffer - may block */
	ret = ids_get(ss->zs->ids, &index, &ss->zs->ssd[ss->id]->wait, b);

	b->src_offset = (uint64_t) index * (uint64_t) ss->zs->rma_mtu;

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &unused);
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: ssd %3d: %s: %s index %u\n", 
			zs_role(ss->zs), ss->id, __func__, block, index);
#endif

	/* Read into buffer */	
	memcpy( (void*)((uintptr_t)ss->zs->rma_buf + (uintptr_t)b->src_offset),
	        (void *)((uintptr_t)zs->ssd_mmap_addr + (uintptr_t)b->ssd_file_offset),
	        b->len);

	/* Release SSD block */
	index_s = (uint32_t) (b->ssd_file_offset / (uint64_t)ss->zs->rma_mtu);
	ids_put(ss->zs->ids_ssd, index_s, NULL);

	/* queue on comm to send NEW_BLOCK msg */
	pthread_mutex_lock(&ss->zs->comm[0]->lock);
	TAILQ_INSERT_TAIL(&ss->zs->comm[0]->q, req, entry);
	pthread_mutex_unlock(&ss->zs->comm[0]->lock);
}

static void 
do_ssd(ssd_stat_t *ss)
{

	zs_globals_t *zs = ss->zs;

	zs_ssd_t *ssd = NULL;
	request_t *req = NULL;

	ssd = zs->ssdq;

	while (!ss->done && !zs->done) {

		pthread_mutex_lock(&zs->master.lock);
	again: 
		if (zs->done ) {
			pthread_mutex_unlock(&zs->master.lock);
			goto next;
		}

		if(!ssd && !zs->done) {

#ifdef DEBUG
			fprintf(stderr, "%s: ssd %2d: %s: no SSD available - sleeping\n",
				zs_role(zs), 0, __func__);
#endif
			waitq_wait_locked(&zs->master.ssd_wq, &zs->ssd[0]->wait);

#ifdef DEBUG
			fprintf(stderr, "%s: ssd %2d: %s: waking\n", zs_role(zs), 0, __func__);
#endif
			goto again;
		}

		pthread_mutex_unlock(&zs->master.lock);

	next:

		if (ss->done || zs->done)
			goto out;

		pthread_mutex_lock(&ssd->lock);
		req = TAILQ_FIRST(&ssd->q);
		if (req) {
			TAILQ_REMOVE(&ssd->q, req, entry);
			ssd->queued--;
		}else {
			ssd->busy = 0;
		}
		pthread_mutex_unlock(&ssd->lock);


		if(!req)
			continue;

		// TODO: read/write as needed 
		if (ss->zs->role == ZS_SRC) {
			read_block_from_ssd(ss, req);
		}
		else{
			write_block_from_ssd(ss, req);
		}

		pthread_mutex_lock(&ssd->lock);
		ssd->busy = 0;
		pthread_mutex_unlock(&ssd->lock);

	}

    out:
	return;	
}

void 
ssd(void *args)
{
	int i = 0;
	int ret = 0;
	ssd_stat_t *ss = NULL;
	zs_globals_t *zs = (zs_globals_t *)args;
	pthread_t tid = pthread_self();

	ss = calloc(1, sizeof(*ss));
	if (!ss){
#ifdef DEBUG
		fprintf(stderr, "%s: no memory for ss\n", __func__);
#endif
		goto out;
	}

	ss->zs = zs;

	// determine who I am 
	ss->id = -1;

	for (i = 0; i < zs->num_ssds; i++) {
		if (pthread_equal(tid, zs->ssd[i]->tid))
			ss->id = i;
	}

	assert(ss->id != -1);

#ifdef DEBUG
	fprintf(stderr, "%s: %2d ready\n", __func__, ss->id);
#endif

	// wait for all threads to start 
	pthread_mutex_lock(zs->lock);
	if (!zs->done){
		zs->ready++;
		waitq_wait_locked(&zs->start, &zs->ssd[ss->id]->wait);
	}
	pthread_mutex_unlock(zs->lock);

	if (!zs->done)
		do_ssd(ss);

    out:
#ifdef DEBUG
	fprintf(stderr, "%s: %2d shutting down\n", __func__, ss->id);
#endif

	free(ss);

	pthread_mutex_lock(zs->lock);
	zs->done++;
	pthread_mutex_unlock(zs->lock);	

	pthread_exit(NULL);
}

