#define _GNU_SOURCE
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <assert.h>
#include <unistd.h>

#include "master.h"
#include "waitq.h"

#if HAVE_LUSTRE_SUPPORT == 1
//#include <lustre/liblustreapi.h>
#include <lustre/lustreapi.h>
#endif

int master_get_ost(zs_globals_t *zs, waiter_t *w, zs_ost_t **ostp, int id)
{
	int ret = 0, i = 0, next = 0, last = 0;
	zs_ost_t *ost = NULL;

	master_state_t *ms = zs->master.priv;

	pthread_mutex_lock(&zs->master.lock);
    again:
	if (zs->done) {
		pthread_mutex_unlock(&zs->master.lock);
		return -1;
	}

	next = ms->next_ost;
	last = next - 1;
	if (last == -1)
		last = zs->num_osts - 1;

	last = next - 1 >= 0 ? next - 1 : zs->num_osts - 1;

/*#ifdef DEBUG
	fprintf(stderr, "%s: io %2d: %s: next %d last %d\n", zs_role(zs), id,
			__func__, next, last);
#endif*/

	for (i = next; i != last; i = (i == zs->num_osts - 1 ? 0 : i + 1)) {
		zs_ost_t *o = zs->osts[i];

		pthread_mutex_lock(&o->lock);
		if (!o->busy && o->queued) {
			o->busy = 1;
			ost = o;
		}
		pthread_mutex_unlock(&o->lock);
		if (ost) {
/*#ifdef DEBUG
			fprintf(stderr, "%s: io %2d: %s: got ost %3d (%p)\n",
					zs_role(zs), id, __func__, i, (void*)ost);
#endif*/
			ms->next_ost = i + 1;
			if (ms->next_ost == zs->num_osts)
				ms->next_ost = 0;
			break;
		}
	}

	if (!ost && !zs->done) {
		if (i == last) {
			zs_ost_t *o = zs->osts[i];

			pthread_mutex_lock(&o->lock);
			if (!o->busy && o->queued) {
				o->busy = 1;
				ost = o;
			}
			pthread_mutex_unlock(&o->lock);
			if (ost) {
/*#ifdef DEBUG
				fprintf(stderr, "%s: io %2d: %s: got ost %3d (%p)\n",
						zs_role(zs), id, __func__, i, (void*)ost);
#endif*/
				ms->next_ost = i + 1;
				if (ms->next_ost == zs->num_osts)
					ms->next_ost = 0;
			}
		}
		if (!ost && !zs->done) {

/*#ifdef DEBUG
			fprintf(stderr, "%s: io %2d: %s: no osts available - sleeping\n",
					zs_role(zs), id, __func__);
#endif*/
			waitq_wait_locked(&zs->master.ios_wq, w);
/*#ifdef DEBUG
			fprintf(stderr, "%s: io %2d: %s: waking\n", zs_role(zs), id, __func__);
#endif*/
			goto again;
		}
	}
	pthread_mutex_unlock(&zs->master.lock);

	*ostp = ost;

	return ret;
}

int master_new_file(master_state_t *ms, zs_file_t *f)
{
	return 0;
}

int master_file_id(master_state_t *ms, zs_file_t *f)
{
	return 0;
}

int master_new_block(master_state_t *ms, zs_file_t *f, zs_block_t *b)
{
	return 0;
}

int master_block_done(master_state_t *ms, zs_block_t *b)
{
	return 0;
}

int master_file_close(master_state_t *ms, zs_file_t *f)
{
	return 0;
}

/* Get set of OSTs for existing file (src only)
 *
 * FIXME: replace code that actually gets the file
 * stripe information including width (ost_cnt),
 * size (ost_mtu), and the array of OSTs.
 */
static int
file_get_osts(master_state_t *ms, zs_file_t *f)
{
	int i = 0, ret = 0;;
	static int next_ost = 0;

	if (ms->zs->role == ZS_SINK) {	/* for sink */

		f->ost_cnt = 1; /* FIXME: get actual count from file */
		f->ost_mtu = 1048576; /* FIXME: get actual mtu from file */
		f->osts = calloc(f->ost_cnt, sizeof(*f->osts));
		if (!f->osts) {
			ret = ENOMEM;
			goto out;
		}

		for (i = 0; i < f->ost_cnt; i++) {
			f->osts[i] = ms->zs->osts[next_ost++];

			if (next_ost == ms->zs->num_osts)
				next_ost = 0;
		}
	}
	else if (ms->zs->role == ZS_SRC) {	/* for source */

#if ENABLE_LUSTRE_SUPPORT == 0
		int i = 0, ret = 0;
		static int next_ost = 0;

		f->ost_cnt = 1; /* FIXME: get actual count from file */
		f->ost_mtu = 1048576; /* FIXME: get actual mtu from file */
		f->osts = calloc(f->ost_cnt, sizeof(*f->osts));
		if (!f->osts) {
			ret = ENOMEM;
			goto out;
		}

		ZS_DEBUG ("file %s getting osts:", f->name);
		for (i = 0; i < f->ost_cnt; i++) {
			f->osts[i] = ms->zs->osts[next_ost++];
			ZS_DEBUG ("\tost %p", (void*)f->osts[i]);

			if (next_ost == ms->zs->num_osts)
				next_ost = 0;
		}
#if 0
//		f->ost_cnt = 4; /* FIXME: get actual count from file */
//		f->ost_mtu = 1048576; /* FIXME: get actual mtu from file */
		f->ost_cnt = 1;
		f->ost_mtu = 4294967296; // 4GB

		f->osts = calloc(f->ost_cnt, sizeof(*f->osts));
		if (!f->osts) {
			ret = ENOMEM;
			goto out;
		}

		for (i = 0; i < f->ost_cnt; i++) {
			f->osts[i] = ms->zs->osts[next_ost++];

			if (next_ost == ms->zs->num_osts)
				next_ost = 0;
		}
#endif
#else
		uint32_t offset, cnt, mtu;

		char *path = calloc(80, sizeof(char));	// the size of file path limited by 80 bytes
		if (!path) {
			ZS_DEBUG ("No memory for path\n");
			goto out;
		}

		sprintf(path, "%s/%s", ms->zs->dir_name, f->name);
		ZS_DEBUG ("Allocating OST stripe info array (size: %d)\n", ZS_NUM_OSTS);

		uint32_t *ost_stripe_info = calloc(ZS_NUM_OSTS, sizeof(uint32_t));	

		/* ost idx can be random (from the first ost offset) */
		ret = get_file_layout_info(path,
		                           &f->ost_offset,
		                           &f->ost_cnt,
		                           &f->ost_mtu,
		                           ost_stripe_info);
		if (ret == -1) {
			fprintf (stderr, "get_file_layout_info() failed\n");
			goto out;
		}

		if (f->ost_cnt == 0) {
			ZS_DEBUG ("%d, %d, %d\n", f->ost_offset, f->ost_cnt, f->ost_mtu);
			ZS_DEBUG ("ost_cnt = 0\n");
			exit(0);
		}

		if(f->ost_cnt > ZS_NUM_OSTS){
			ZS_DEBUG ("f->ost_cnt: %d > ZS_NUM_OSTS\n",
			          f->ost_offset, ZS_NUM_OSTS);
			goto out;
		}

		f->osts = calloc(f->ost_cnt, sizeof(*f->osts));
		if (!f->osts) {
			ret = ENOMEM;
			goto out;
		}

		for (i = 0; i < f->ost_cnt; i++) {
			next_ost = ost_stripe_info[i];
			next_ost = next_ost % f->ost_cnt;  //// TODO : Error in striping count = 1
			f->osts[i] = ms->zs->osts[next_ost];
		}

		free(path);
		free (ost_stripe_info);
#endif
	}

out:
	return ret;
}

/* Open the src dir, create file struct for each file, open the file,
 * append to comm's queue to request the sink fd.
 */
static int
src_add_files(master_state_t *ms)
{
	int ret = 0, fd = 0;
	zs_globals_t *zs = ms->zs;
	DIR *dir = NULL;
	struct dirent *dp = NULL;

	dir = opendir(zs->dir_name);
	if (!dir) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: opendir(%s) failed with %s\n",
				__func__, zs->dir_name, strerror(errno));
#endif*/
		ret = ENOMEM;
		goto out;
	}

	/* get the number of files */
	while ((dp = readdir(dir)) != NULL) {
		if (dp->d_name[0] == '.')
			continue;

		/* no need to take lock, since no one else checks it until
		 * the transfer is complete
		 */
		zs->num_files++;
	}

	ZS_DEBUG ("%d files to transfer\n", zs->num_files);
	if (zs->num_files <= 0) {
		fprintf (stderr, "No file to transfer\n");
		return -1;
	}

	/* reset to beginning */
	rewinddir(dir);

	/* we should have a limit of open files to avoid running out of
	   descriptors. */
	while ((dp = readdir(dir)) != NULL) {
		zs_file_t *f = NULL;
		char name[FILENAME_MAX];
		struct stat s;

		if (dp->d_name[0] == '.')
			continue;

		memset(name, 0, sizeof(name));
		snprintf(name, sizeof(name), "%s/%s", zs->dir_name, dp->d_name);
		ZS_DEBUG ("Transferring %s\n", name);

#if ENABLE_LUSTRE_SUPPORT == 1
		fd = open(name, O_RDONLY|O_DIRECT);
#else
		fd = open(name, O_RDONLY);
#endif
		assert(fd != -1); /* TODO start work and add new files after closing one */

		fstat(fd, &s);

		f = calloc(1, sizeof(*f));
		assert(f); /* TODO add warning, cleanup */

		f->req.type = REQ_NEW_FILE;
		f->name = strdup(dp->d_name);
		assert(f->name); /* TODO add warning, cleanup */

		f->len = s.st_size;
		f->src_fd = fd;
		f->num_blocks = f->len / zs->rma_mtu;
		if (f->len != zs->rma_mtu * f->num_blocks)
			f->num_blocks++;

		ret = file_get_osts(ms, f);
		assert(ret == 0);

		ret = fd_tree_add(zs->ftree, f->src_fd, f);
		assert(ret == 0);

		pthread_mutex_lock(&zs->comm[0]->lock);
		TAILQ_INSERT_TAIL(&zs->comm[0]->q, &f->req, entry);
		pthread_mutex_unlock(&zs->comm[0]->lock);
	}

	closedir(dir);

out:
	return ret;
}

static void
src_schedule_file(master_state_t *ms, request_t *req)
{
	int             i           = 0;
	int             ret         = 0;
	int             unused      = 0;
	zs_file_t       *f          = container_of(req, zs_file_t, req);
	char            block[128];
	zs_globals_t    *zs         = ms->zs;

	ZS_DEBUG ("%s: master: %s\n", zs_role(ms->zs), __func__);

	for (i = 0; i < f->num_blocks; i++) {
		zs_block_t *b = NULL;
		zs_ost_t *ost = f->osts[i % f->ost_cnt];

		ZS_DEBUG ("OST target: %d (OST count: %d)\n",
		          i % f->ost_cnt, f->ost_cnt);
		assert(ost);

		b = calloc(1, sizeof(*b));
		if (!b) {
			f->pending = i;
			pthread_mutex_lock(&ms->zs->master.lock);
			TAILQ_INSERT_HEAD(&ms->zs->master.q, req, entry);
      //printf("size of entry : %ld", sizeof(entry));
			pthread_mutex_unlock(&ms->zs->master.lock);
			goto out;
		}
		b->req.type = REQ_NEW_BLOCK;
		b->file = f;
		b->ost = ost;

		/* FIXME assume ost_mtu == rma_mtu for now */
		b->file_offset = (uint64_t)i * (uint64_t)ms->zs->rma_mtu;
		b->len = ms->zs->rma_mtu; /* for now */
		if (i == (f->num_blocks - 1))
			b->len = f->len - (i * ms->zs->rma_mtu);
		/* b->src_offset will be based on buffer id */
		/* b->sink_offset is only set on sink */

		memset(block, 0, sizeof(block));
		ret = sprint_block(b, block, sizeof(block), &unused);
/*#ifdef DEBUG
		if (!ret)
			fprintf(stderr, "%s: master: %s: schedule %s\n",
					zs_role(ms->zs), __func__, block);
#endif*/

		fflush (stderr);

		pthread_mutex_lock(&ost->lock);
		TAILQ_INSERT_TAIL(&ost->q, &b->req, entry);
		ost->queued++;
		pthread_mutex_unlock(&ost->lock);
	}

out:
	/* we have scheduled new blocks, wake IO threads */
	for (i = 0; i < f->ost_cnt; i++)
		waitq_signal(&ms->zs->master.ios_wq);

	if (zs->ssd_buffer_flag == 1)
		waitq_signal(&ms->zs->master.ssd_wq);

	return;
}

static void
progress_src_requests(master_state_t *ms)
{
	zs_globals_t *zs = ms->zs;
	pthread_mutex_t *lock = &zs->master.lock;
	request_t *req = NULL;

	pthread_mutex_lock(lock);
    again:
	req = TAILQ_FIRST(&zs->master.q);
	if (req) {
		TAILQ_REMOVE(&zs->master.q, req, entry);
	} else {
/*#ifdef DEBUG
		fprintf(stderr, "%s: master: %s: no requests available - sleeping\n",
				zs_role(zs), __func__);
#endif*/
		waitq_wait_locked(&zs->master.m_wq, &zs->master.wait);
/*#ifdef DEBUG
		fprintf(stderr, "%s: master: %s: waking\n", zs_role(zs), __func__);
#endif*/
		if (!zs->done)
			goto again;
	}
	pthread_mutex_unlock(lock);

	if (!req || zs->done) {
		return;
	}

	switch (req->type) {
	case REQ_FILE_ID:
		src_schedule_file(ms, req);
		break;
	default:
		fprintf(stderr, "%s: ignoring request %s\n", __func__,
				request_type_str(req->type));
	}

	return;
}

static int
do_src(master_state_t *ms)
{
	int ret = 0;

	/* create files and pass to comm to request sink_fd */
	ret = src_add_files(ms);
	if (ret) {
		ZS_DEBUG ("%s: src_add_files() failed - exiting\n", __func__);
		ms->zs->done++;
		return -1;
	}

	/* handle requests until done */
	while (!ms->zs->done)
		progress_src_requests(ms);

  //printf("\nSource side master end\n");
	return 0;
}

static void
handle_new_file(master_state_t *ms, request_t *req)
{
	int ret = 0;
	char name[FILENAME_MAX];
	zs_file_t *f = container_of(req, zs_file_t, req);

/*#ifdef DEBUG
	fprintf(stderr, "%s: master: %s\n", zs_role(ms->zs), __func__);
#endif*/

	memset(name, 0, sizeof(name));
	snprintf(name, sizeof(name), "%s/%s", ms->zs->dir_name, f->name);

#if ENABLE_LUSTRE_SUPPORT == 1
	f->sink_fd = open(name, O_WRONLY|O_CREAT|O_TRUNC|O_DIRECT, 0644);
#else
    f->sink_fd = open(name, O_WRONLY|O_CREAT|O_TRUNC, 0644);
#endif

	if (f->sink_fd == -1) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: open(%s) failed with %s\n", __func__,
				name, strerror(errno));
#endif*/
	}
	assert(f->sink_fd != -1); /* TODO start work and add new files after closing one */

	ret = file_get_osts(ms, f);
	assert(ret == 0);

	ret = fd_tree_add(ms->zs->ftree, f->sink_fd, f);
	assert(ret == 0);

	/* queue for comm to send FILE_ID */
	req->type = REQ_FILE_ID;

	pthread_mutex_lock(ms->zs->lock);
	ms->zs->num_files++;
	pthread_mutex_unlock(ms->zs->lock);

	pthread_mutex_lock(&ms->zs->comm[0]->lock);
	TAILQ_INSERT_TAIL(&ms->zs->comm[0]->q, req, entry);
	pthread_mutex_unlock(&ms->zs->comm[0]->lock);

	return;
}

static void
handle_new_block(master_state_t *ms, request_t *req)
{
	int ret = 0;
	uint32_t index = 0;
	zs_block_t *b = container_of(req, zs_block_t, req);

/*#ifdef DEBUG
	fprintf(stderr, "%s: master: %s\n", zs_role(ms->zs), __func__);
#endif*/

	/* try to get RMA buffer slot */
	ret = ids_get(ms->zs->ids, &index, &ms->zs->master.wait, b);
	assert(ret == 0);

	/* got it, RMA Read the data */
	b->sink_offset = (uint64_t) index * (uint64_t) ms->zs->rma_mtu;

	/* queue on comm thread to send BLOCK_DONE */
	pthread_mutex_lock(&ms->zs->comm[0]->lock);
	TAILQ_INSERT_TAIL(&ms->zs->comm[0]->q, req, entry);
	pthread_mutex_unlock(&ms->zs->comm[0]->lock);

	return;
}

static void
progress_sink_requests(master_state_t *ms)
{
	zs_globals_t *zs = ms->zs;
	pthread_mutex_t *lock = &zs->master.lock;
	request_t *req = NULL;

	pthread_mutex_lock(lock);
    again:
	req = TAILQ_FIRST(&zs->master.q);
	if (req) {
		TAILQ_REMOVE(&zs->master.q, req, entry);
	} else {
/*#ifdef DEBUG
		fprintf(stderr, "%s: master: %s: no requests available - sleeping\n",
				zs_role(zs), __func__);
#endif*/
		waitq_wait_locked(&zs->master.m_wq, &zs->master.wait);
/*#ifdef DEBUG
		fprintf(stderr, "%s: master: %s: waking\n", zs_role(zs), __func__);
#endif*/
		if (!zs->done)
			goto again;
	}
	pthread_mutex_unlock(lock);

	if (!req || zs->done)
		return;

/*#ifdef DEBUG
	fprintf(stderr, "%s: master: %s: found %s (%p)\n", zs_role(ms->zs), __func__,
			request_type_str(req->type), (void*)req);
#endif*/

	switch (req->type) {
	case REQ_NEW_FILE:
		handle_new_file(ms, req);
		break;
	case REQ_NEW_BLOCK:
	case REQ_BLOCK_DONE:
		handle_new_block(ms, req);
		break;
	default:
		fprintf(stderr, "%s: ignoring request %s\n", __func__,
				request_type_str(req->type));
	}

	return;
}

static void
do_sink(master_state_t *ms)
{
	while (!ms->zs->done)
		progress_sink_requests(ms);

	return;
}

void
master(void *args)
{
	int             ret = 0;
	zs_globals_t    *zs = (zs_globals_t *)args;
	master_state_t  *ms = NULL;

	ms = calloc(1, sizeof(*ms));
	if (!ms) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: no memory for ms\n", __func__);
#endif*/
		goto out;
	}

	ms->zs = zs;
	zs->master.priv = ms;

	TAILQ_INIT(&zs->master.q);
	waitq_init(&zs->master.m_wq, &zs->master.lock);
	waitq_init(&zs->master.ios_wq, &zs->master.lock);
	if (zs->ssd_buffer_flag == 1)
		waitq_init(&zs->master.ssd_wq, &zs->master.lock);

	ret = pthread_mutex_init(&ms->ost_lock, NULL);
	if (ret) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: pthread_mutex_init() failed with %s\n",
				__func__, strerror(ret));
#endif*/
		goto out;
	}

#ifdef DEBUG
	fprintf(stderr, "%s: ready\n", __func__);
#endif

	/* wait for all threads to start */
	pthread_mutex_lock(zs->lock);
	if (!zs->done) {
		zs->ready++;
		waitq_wait_locked(&zs->start, &zs->master.wait);
	}
	pthread_mutex_unlock(zs->lock);

	if (zs->done)
		goto out;

	/* the connection is open, all threads are up, let's go */
	if (zs->role == ZS_SRC) {
		ret = do_src(ms);
		if (ret)
			ZS_DEBUG ("do_src() failed\n");
	}
  else {
		do_sink(ms);
	}

    out:
#ifdef DEBUG
	fprintf(stderr, "%s: shutting down\n", __func__);
#endif

	free(ms);
	zs->master.priv = NULL;

	pthread_mutex_lock(zs->lock);
	zs->done++;
	pthread_mutex_unlock(zs->lock);

	/* XXX If there is no file to transfer, the following function will
	       cause a segfault */
	waitq_broadcast(&zs->master.ios_wq);
	if (zs->ssd_buffer_flag == 1)
		waitq_broadcast(&zs->master.ssd_wq);

	ids_destroy(zs->ids);
	if (zs->ssd_buffer_flag == 1)
		ids_destroy(zs->ids_ssd);

	pthread_exit(NULL);
}
