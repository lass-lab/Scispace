#include <sys/queue.h>
#include <unistd.h>
#include <assert.h>
#include <sys/time.h>
#include <inttypes.h>

#include "comm.h"
#include "request.h"
#include "wire.h"

extern zs_globals_t *zs;

/* For now, we decide on which connection to send a message using a
   round-robin algorithm so we need to track the index of the last
   connection used */
int last_conn_index = -1;

/* Function used to know what connection to use */
static inline int
get_conn_index (comm_state_t *cs)
{
	int index = last_conn_index;

	if (zs->num_conns == 1)
		return 0;

	/* The array of connections can be sparse at some point during
	   execution */
	do {
		last_conn_index++;
		if (last_conn_index == zs->num_conns)
			last_conn_index = 0;
		if (last_conn_index == index) {
			/* We looped over the connections and did not find a
			   candidate, return an error */
			return -1;
		}
	} while (cs->connections[last_conn_index] == NULL);
	
	return (last_conn_index);
}

void
release_file(zs_globals_t *zs, request_t *req)
{
	zs_file_t *f = container_of(req, zs_file_t, req);
	int fd = zs->role == ZS_SRC ? f->src_fd : f->sink_fd;

/*
#ifdef DEBUG
	fprintf(stderr, "%s: %s: file %s fd %d num_blocks %u completed %u\n",
			zs_role(zs), __func__, f->name,
			zs->role == ZS_SRC ? f->src_fd : f->sink_fd,
			f->num_blocks, f->completed);
#endif
*/

	pthread_mutex_lock(zs->lock);
	zs->files_completed++;
	if (zs->files_completed == zs->num_files &&
	    (zs->role == ZS_SRC || zs->comm_done)) {
		zs->done++;
	}
	pthread_mutex_unlock(zs->lock);
	if (zs->done) {
		waitq_broadcast(&zs->master.m_wq);
		waitq_broadcast(&zs->master.ios_wq);
		if (zs->ssd_buffer_flag == 1)
			waitq_broadcast(&zs->master.ssd_wq);
	}
	fd_tree_remove(zs->ftree, fd, (void*)f);
	close(fd);
	free(f->osts);
	free(f->name);
	free(f);
	/* TODO wake master if need to open another file?
	 *      if so, what do we queue on the master's q?
	 *      An empty request of type FILE_DONE?
	 */

	return;
}

void release_block(zs_globals_t *zs, request_t *req)
{
	int ret = 0, offset = 0;
	zs_block_t *b = container_of(req, zs_block_t, req);
	char block[192];

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &offset);
/*
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: %s: %s\n", zs_role(zs), __func__, block);
#endif
*/

	free(b);

	return;
}

static void
comm_handle_send(comm_state_t *cs, cci_event_t *event)
{
	pthread_mutex_t *lock = &cs->zs->comm[cs->id]->lock;
	request_t *req = event->send.context;

	if (event->send.status == CCI_ERR_DISCONNECTED) {
		ZS_DEBUG ("%s: disconnected, shutting down\n", __func__);
		cs->done++;
		return;
	}

	assert(req != NULL);

	/* READY has no context */
	if (!req)
		return;

/*
#ifdef DEBUG
  printf("Shit the fuck\n");
	fprintf(stderr, "%s: comm %d: %s: completed %s send status %s\n",
			zs_role(cs->zs), cs->id, __func__, request_type_str(req->type),
			cci_strerror(cs->endpoint, event->send.status));
#endif
*/

	switch (req->type) {
	case REQ_FILE_DONE:
		if (event->send.status == CCI_SUCCESS) {
			release_file(cs->zs, req);
		}
		/* fall through */
	case REQ_NEW_FILE:
	case REQ_FILE_ID:
	case REQ_NEW_BLOCK:
		pthread_mutex_lock(lock);
		if (event->send.status != CCI_SUCCESS) {
/*
#ifdef DEBUG
			fprintf(stderr, "%s: comm %d: %s: requeuing %s (%p)\n",
					zs_role(cs->zs), cs->id, __func__,
					request_type_str(req->type), (void*)req);
#endif
*/

			TAILQ_INSERT_HEAD(&cs->zs->comm[cs->id]->q, req, entry);
		}
		pthread_mutex_unlock(lock);
		break;
	case REQ_BLOCK_DONE:
	{
		int offset = 0;
		zs_block_t *b = container_of(req, zs_block_t, req);
		char block[192];

		memset(block, 0, sizeof(block));
		sprint_block(b, block, sizeof(block), &offset);

		/* We have RMA Read the data, enqueue on ost,
		 * wake io thread to write it out */
		if (event->send.status == CCI_SUCCESS) {
/*
#ifdef DEBUG
			fprintf(stderr, "%s: comm %d: %s: RMA read %s\n",
					zs_role(cs->zs), cs->id, __func__, block);
#endif
*/
			pthread_mutex_lock(&b->ost->lock);
			TAILQ_INSERT_TAIL(&b->ost->q, req, entry);
			b->ost->queued++;
			pthread_mutex_unlock(&b->ost->lock);
			waitq_signal(&cs->zs->master.ios_wq);
//			if (zs->ssd_buffer_flag == 1)
//				waitq_signal(&cs->zs->master.ssd_wq);
		} else {
/*
#ifdef DEBUG
			fprintf(stderr, "%s: comm %d: %s: RMA read %s failed with %s\n",
					zs_role(cs->zs), cs->id, __func__, block,
					cci_strerror(cs->endpoint, event->send.status));
#endif
*/
			pthread_mutex_lock(lock);
			TAILQ_INSERT_HEAD(&cs->zs->comm[cs->id]->q, req, entry);
			pthread_mutex_unlock(lock);
		}
		break;
	}
	case REQ_BYE:
		pthread_mutex_lock(lock);
		cs->done++;
		pthread_mutex_unlock(lock);
		break;
	default:
/*
#ifdef DEBUG
		fprintf(stderr, "%s: ignoring request type %d\n", __func__, req->type);
#endif
*/
		assert(1);
	}

	return;
}

static void
comm_handle_new_file(comm_state_t *cs, cci_event_t *event)
{
	zs_file_t *f = NULL;
	uint32_t name_len = 0;
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/

	assert(cs->zs->role == ZS_SINK);

	f = calloc(1, sizeof(*f));
	/* how should we handle an error?
	 * we can't queue it because there is no memory.
	 * we could try to reply, but what if that fails?
	 * for now, we die.
	 */
	assert(f);

	f->req.type = REQ_NEW_FILE;
	pthread_mutex_init(&f->lock, NULL);

	parse_new_file((void*)event->recv.ptr, event->recv.len,
			&name_len, &f->len, (uint32_t *)&f->src_fd, &f->name);

	f->num_blocks = f->len / cs->zs->rma_mtu;
	if (f->len != cs->zs->rma_mtu * f->num_blocks)
		f->num_blocks++;

	pthread_mutex_lock(&cs->zs->master.lock);
	TAILQ_INSERT_TAIL(&cs->zs->master.q, &f->req, entry);
	pthread_mutex_unlock(&cs->zs->master.lock);
	waitq_signal(&cs->zs->master.m_wq);

	return;
}

static void
comm_handle_file_id(comm_state_t *cs, cci_event_t *event)
{
	int ret = 0;
	uint32_t src_fd = 0, sink_fd = 0;
	request_t *req = NULL;
	zs_file_t *f = NULL;
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	assert(cs->zs->role == ZS_SRC);

	ret = parse_file_id((void*)event->recv.ptr, event->recv.len, &src_fd, &sink_fd);
	assert(ret == 0);

	ret = fd_tree_find(cs->zs->ftree, src_fd, (void**)&req);
	assert(ret == 0);
	f = container_of(req, zs_file_t, req);

	f->req.type = REQ_FILE_ID;
	f->sink_fd = sink_fd;

	pthread_mutex_lock(&cs->zs->master.lock);
	TAILQ_INSERT_TAIL(&cs->zs->master.q, req, entry);
	pthread_mutex_unlock(&cs->zs->master.lock);
	waitq_signal(&cs->zs->master.m_wq);

	return;
}

static int
send_block_done(comm_state_t *cs, request_t *req);

static void
comm_handle_new_block(comm_state_t *cs, cci_event_t *event)
{
	int ret = 0, offset = 0;
	request_t *req = NULL;
	zs_block_t *b = NULL;
	zs_file_t *f = NULL;
	uint32_t sink_fd = 0, ost_idx = 0, index = 0;
	char block[192];
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	assert(cs->zs->role == ZS_SINK);

	b = calloc(1, sizeof(*b));
	/* how should we handle an error?
	 * we can't queue it because there is no memory.
	 * we could try to reply, but what if that fails?
	 * for now, we die.
	 */
	assert(b);
	b->req.type = REQ_BLOCK_DONE;

	parse_block((void*)event->recv.ptr, event->recv.len,
			&sink_fd, &b->file_offset,
			&b->src_offset, &b->len);

	ret = fd_tree_find(cs->zs->ftree, sink_fd, (void**)&req);
	assert(ret == 0);

	f = container_of(req, zs_file_t, req);
	b->file = f;

	ost_idx = (uint32_t) (b->file_offset / (uint64_t) cs->zs->rma_mtu);
	ost_idx %= f->ost_cnt;

	b->ost = f->osts[ost_idx];

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &offset);
/*
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: comm %d: %s: assigning index %u %s\n",
				zs_role(cs->zs), cs->id, __func__, ost_idx, block);
#endif
*/
	assert(b->ost >= cs->zs->osts[0] &&
			b->ost <= cs->zs->osts[cs->zs->num_osts - 1]);

	/* try to get RMA buffer slot */
	ret = ids_try_get(cs->zs->ids, &index, b);

	if (ret == 0) {
		/* got it, RMA Read the data */
		b->sink_offset = (uint64_t) index * (uint64_t) cs->zs->rma_mtu;
		ZS_DEBUG ("Sending block done (sink offset: %lu)", b->sink_offset);
		ret = send_block_done(cs, &b->req);
		if (ret) {
			pthread_mutex_lock(&cs->zs->comm[cs->id]->lock);
			TAILQ_INSERT_HEAD(&cs->zs->comm[cs->id]->q, &b->req, entry);
			pthread_mutex_unlock(&cs->zs->comm[cs->id]->lock);
		}
	} else {
		/* none available, queue for master */
		pthread_mutex_lock(&cs->zs->master.lock);
		TAILQ_INSERT_TAIL(&cs->zs->master.q, &b->req, entry);
		pthread_mutex_unlock(&cs->zs->master.lock);
		waitq_signal(&cs->zs->master.m_wq);
	}

	return;
}

static void
comm_handle_block_done(comm_state_t *cs, cci_event_t *event)
{
	int ret = 0, offset = 0;
	zs_file_t *f = NULL;
	zs_block_t *b = NULL;
	uint32_t src_fd = 0, index = 0;
	uint64_t file_offset = 0, src_offset = 0;
	char buf[192];

	assert(cs->zs->role == ZS_SRC);

	parse_done((void*)event->recv.ptr, event->recv.len,
			&src_fd, &file_offset, &src_offset);

	index = (uint32_t) (src_offset / (uint64_t)cs->zs->rma_mtu);
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm %d: %s: file_offset %"PRIu64" index %u "
			"src_offset %"PRIu64" src_fd %u\n", zs_role(cs->zs),
			cs->id, __func__, file_offset, index, src_offset, src_fd);
#endif
*/
	ret = ids_get_context(cs->zs->ids, index, (void**)&b);
	assert(ret == 0 && b != NULL);
	f = b->file;

	ids_put(cs->zs->ids, index, b);

	memset(buf, 0, sizeof(buf));
	ret = sprint_block(b, buf, sizeof(buf), &offset);
/*
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: comm %d: %s: %s index %u\n", zs_role(cs->zs),
				cs->id, __func__, buf, index);
#endif
*/
	release_block(cs->zs, &b->req);

	pthread_mutex_lock(&f->lock);
	f->completed++;
	pthread_mutex_unlock(&f->lock);

	if (f->completed == f->num_blocks)
		release_file(cs->zs, &f->req);

	return;
}

static void
comm_handle_file_close(comm_state_t *cs, cci_event_t *event)
{
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	assert(cs->zs->role == ZS_SINK);
	return;
}

static void
comm_handle_recv(comm_state_t *cs, cci_event_t *event)
{
	hdr_t *hdr = (hdr_t*) event->recv.ptr;
	uint32_t type = ntohl(hdr->type);

	switch (type) {
	case READY:
		assert(cs->zs->role == ZS_SRC);
		cs->ready = 1;
		break;
	case NEW_FILE:
		comm_handle_new_file(cs, event);
		break;
	case FILE_ID:
		comm_handle_file_id(cs, event);
		break;
	case NEW_BLOCK:
		comm_handle_new_block(cs, event);
		break;
	case BLOCK_DONE:
		comm_handle_block_done(cs, event);
		break;
	case FILE_CLOSE:
		comm_handle_file_close(cs, event);
		break;
	case BYE: {
		uint8_t i;

		for (i = 0; i < zs->num_conns; i++) {
			cci_disconnect(cs->connections[i]);
			cs->connections[i] = NULL;
		}
		pthread_mutex_lock(cs->zs->lock);
		cs->zs->comm_done++;
		pthread_mutex_unlock(cs->zs->lock);
		if (cs->zs->files_completed == cs->zs->num_files) {
			pthread_mutex_lock(cs->zs->lock);
			cs->zs->done++;
			pthread_mutex_unlock(cs->zs->lock);
			waitq_broadcast(&cs->zs->master.m_wq);
			waitq_broadcast(&cs->zs->master.ios_wq);
//			if (zs->ssd_buffer_flag == 1)
//				waitq_broadcast(&cs->zs->master.ssd_wq);
		}
		break;
	}
	default:
/*
#ifdef DEBUG
		fprintf(stderr, "%s: ignoring unknown msg type %d\n", __func__, type);
#endif
*/
		assert(1);
	}

	return;
}

static void
comm_handle_connect_request(comm_state_t *cs, cci_event_t *event)
{
	int ret = 0;
	uint32_t blocksize = 0, num_blocks = 0, list_len = 0;
	zs_globals_t *zs = cs->zs;

	assert(cs->zs->role == ZS_SINK);

	ret = parse_connect((void*)event->request.data_ptr, event->request.data_len,
			&blocksize, &num_blocks, &list_len,
			(void*)&cs->remote);
	assert(ret == 0);
	assert(list_len == 0);
	assert(blocksize == zs->rma_mtu); /* for now */

	ret = cci_accept(event, NULL);
/*#ifdef DEBUG
	if (ret) {
		fprintf(stderr, "%s: accept() failed with %s\n", __func__,
				cci_strerror(cs->endpoint, ret));
	}
#endif*/

	return;
}

static int
send_connect(comm_state_t *cs)
{
	int ret = 0;
	char buf[192];
	uint32_t send_len = 0;
	zs_globals_t *zs = cs->zs;

	ret = pack_connect(buf, sizeof(buf), zs->rma_mtu, zs->rma_cnt, 0,
			cs->local, &send_len);
	assert(ret == 0);

	if (!cs->attempt)
		cs->attempt = 1;
	else
		cs->attempt *= 2;

	if (cs->attempt)
		sleep(cs->attempt);
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	ret = cci_connect(cs->endpoint, zs->server_uri, buf, send_len,
			CCI_CONN_ATTR_RU, NULL, 0, NULL);

	return ret;
}

static int
comm_handle_accept(comm_state_t *cs, cci_event_t *event)
{
	int rc = CCI_SUCCESS;

	if (event->accept.status == CCI_SUCCESS) {
		int i = 0;

		while (i < zs->num_conns && cs->connections[i] != NULL)
			i++;

		if (i == zs->num_conns) {
			fprintf (stderr, "ERROR: no available slot for incoming connection request\n");
			return CCI_ERROR;
		}

		cs->connections[i] = event->accept.connection;
		/* NOTE: if we want to get the file list, we could
		 * RMA READ it before sending ready.
		 *
		 * For now, just handle the files as they come in.
		 */
		/* send_ready(comm); */
	} else {
		cs->done++;
/*
#ifdef DEBUG
		fprintf(stderr, "%s: accept failed with %s\n", __func__,
				cci_strerror(cs->endpoint, event->accept.status));
#endif
*/
		rc = CCI_ERROR;
	}

	return rc;
}

static int
comm_handle_connect(comm_state_t *cs, cci_event_t *event)
{
	int ret = CCI_SUCCESS;

	assert(cs->zs->role == ZS_SRC);
	if (event->connect.status == CCI_SUCCESS) {
		/* Store the connection in the first available spot */
		uint8_t index = 0;
		while (index < zs->num_conns && cs->connections[index] != NULL) {
			index++;
		}
		cs->connections[index] = event->connect.connection;
	} else {
#if 0
		int rc;
#ifdef DEBUG
		fprintf(stderr, "%s: connect failed with %s\n", __func__,
				cci_strerror(cs->endpoint, event->connect.status));
#endif
		/* Could not connect to the remote peer, we retry */
		ret = send_connect(cs);
#endif
		ret = CCI_ERROR;
	}

	return ret;
}

static int
open_connections (comm_state_t *cs)
{
	int             ret 		= 0;
	zs_globals_t    *zs 		= cs->zs;
	int             active_conns	= 0;
	int             i;

	ZS_DEBUG ("Number of connections to establish: %d\n", zs->num_conns);

	if (zs->role == ZS_SRC) {
		/* We initiate one connection and only one at a time.
		   So we will wait for that connection to be established
		   before initiating another one */
		ret = send_connect(cs);
		if (ret != CCI_SUCCESS) {
			fprintf (stderr, "send_connect() failed\n");
		}
	}

	while (active_conns < zs->num_conns) {
		cci_event_t *event = NULL;

		ret = cci_get_event(cs->endpoint, &event);
		if (ret) {
			if (ret != CCI_EAGAIN) {
/*#ifdef DEBUG
				fprintf(stderr, "%s: cci_get_event() failed with %s\n",
						__func__,
						cci_strerror(cs->endpoint, ret));
#endif*/
			}
			continue;
		}
		assert(event);

		switch (event->type) {
		case CCI_EVENT_CONNECT_REQUEST:
			comm_handle_connect_request(cs, event);
			break;
		case CCI_EVENT_ACCEPT:
			ret = comm_handle_accept(cs, event);
			if (ret == CCI_SUCCESS) {
				active_conns++;
				ZS_DEBUG ("We now have %d active connections\n", active_conns);
			}
			break;
		case CCI_EVENT_CONNECT:
			ret = comm_handle_connect(cs, event);
			if (ret == CCI_SUCCESS) {
				active_conns++;
				ZS_DEBUG ("We now have %d active connections\n", active_conns);
				if (zs->role == ZS_SRC && active_conns < zs->num_conns) {
					ZS_DEBUG ("Initiating another connection...\n");
					ret = send_connect(cs);
					if (ret != CCI_SUCCESS) {
						fprintf (stderr, "send_connect() failed");
					}
				}
			} else {
				/* The connect failed, we retry */
				/* XXX we should not retry indefinitely */
				while (send_connect (cs) != CCI_SUCCESS) {
					sleep (1);
					ret = send_connect(cs);
					if (ret != CCI_SUCCESS) {
						fprintf (stderr, "WARN: send_connect() failed\n");
					}
				}
			}
			break;
		default:
/*#ifdef DEBUG
			fprintf(stderr, "%s: unhandled event %s\n",
					__func__,
					cci_event_type_str(event->type));
#endif*/
			return CCI_ERROR;
		}

		cci_return_event (event);
	}
	cs->ready = 1;

	return ret;
}

#if 0
static void
send_ready(comm_state_t *cs)
{
	int ret = 0;
	hdr_t hdr;

	assert(cs->zs->role == ZS_SINK);

	pack_ready((void*)&hdr, sizeof(hdr.ready));

	/* not queuing req for now */
	cci_send(comm->connection, &hdr, sizeof(hdr.ready), NULL, 0);
	assert(ret == 0);

	return;
}
#endif

static void
progress_comm(comm_state_t *cs)
{
	int ret = 0;
	zs_globals_t *zs = cs->zs;
	cci_endpoint_t *endpoint = cs->endpoint;
	cci_event_t *event = NULL;

	ret = cci_get_event(endpoint, &event);
	if (ret) {
		if (ret != CCI_EAGAIN) {
/*#ifdef DEBUG
			fprintf(stderr, "%s: cci_get_event() failed with %s\n",
					__func__, cci_strerror(endpoint, ret));
#endif*/
			if (ret == CCI_ERR_DEVICE_DEAD)  {
/*#ifdef DEBUG
				fprintf(stderr, "%s: exiting\n", __func__);
#endif*/
				pthread_mutex_lock(&zs->comm[cs->id]->lock);
				cs->done++;
				pthread_mutex_unlock(&zs->comm[cs->id]->lock);
			}
		}
		return;
	}

	switch (event->type) {
		case CCI_EVENT_SEND:
			comm_handle_send(cs, event);
			break;
		case CCI_EVENT_RECV:
			comm_handle_recv(cs, event);
			break;
		default:
/*
#ifdef DEBUG
			fprintf(stderr, "%s: ignoring event %s\n", __func__,
					cci_event_type_str(event->type));
#endif
*/
			break;
	}

	ret = cci_return_event(event);
	if (ret) {
/*#ifdef DEBUG
		fprintf(stderr, "%s: cci_return_event() failed with %s\n",
				__func__, cci_strerror(endpoint, ret));
#endif*/
	}

	return;
}

static int
send_new_file(comm_state_t *cs, request_t *req)
{
	uint32_t    send_len = 0;
	zs_file_t   *new = container_of(req, zs_file_t, req);
	int         conn_index;
	char        buf[192];
	int         rc;

	assert(cs->zs->role == ZS_SRC);

	rc = pack_new_file (buf,
	                    sizeof(buf),
	                    strlen(new->name),
	                    new->len,
	                    new->src_fd, 
	                    new->name,
	                    &send_len);
	if (rc != 0)
		return CCI_ERROR;

	assert(send_len <= sizeof(buf));
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	/* not queuing req for now */
	conn_index = get_conn_index (cs);
	if (conn_index == -1)
		return CCI_ERROR;
	rc = cci_send(cs->connections[conn_index], buf, send_len, req, 0);

	return rc;
}

static int
send_file_id(comm_state_t *cs, request_t *req)
{
	int ret = 0;
	char buf[16];
	uint32_t send_len = 0;
	int conn_index;
	zs_file_t *f = container_of(req, zs_file_t, req);

	assert(cs->zs->role == ZS_SINK);

	pack_file_id(buf, sizeof(buf), f->src_fd, f->sink_fd, &send_len);

	assert(send_len <= sizeof(buf));
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm %d: %s file %s fd %d\n", zs_role(cs->zs),
			cs->id, __func__, f->name, f->sink_fd);
#endif
*/
	conn_index = get_conn_index (cs);
	if (conn_index == -1)
		return CCI_ERROR;
	ret = cci_send(cs->connections[conn_index], buf, send_len, req, 0);
/*#ifdef DEBUG
	if (ret)
		fprintf(stderr, "%s: comm %d: %s: cci_send() failed with %s\n",
				zs_role(cs->zs), cs->id, __func__,
				cci_strerror(cs->endpoint, ret));
#endif*/

	return ret;
}

static int
send_new_block(comm_state_t *cs, request_t *req)
{
	int ret = 0, offset = 0;
	uint32_t send_len = 0;
	int conn_index;
	zs_block_t *new = container_of(req, zs_block_t, req);
	zs_file_t *f = new->file;
	char buf[32], block[192];

	assert(cs->zs->role == ZS_SRC);

	memset(block, 0, sizeof(block));
	ret = sprint_block(new, block, sizeof(block), &offset);
/*
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: comm %d: %s: %s\n", zs_role(cs->zs),
				cs->id, __func__, block);
#endif
*/
	pack_block(buf, sizeof(buf), f->sink_fd, new->file_offset,
			new->src_offset, new->len, &send_len);

	assert(send_len <= sizeof(buf));

	ZS_DEBUG ("%s: comm: %s\n", zs_role(cs->zs), __func__);

	conn_index = get_conn_index (cs);
	if (conn_index == -1) {
		ZS_DEBUG ("Cannot get a valid connection\n");
		return CCI_ERROR;
	}

	ZS_DEBUG ("Sending block on connection %d\n", conn_index);
	ret = cci_send(cs->connections[conn_index], buf, send_len, req, 0);
	if (ret != CCI_SUCCESS) {
		ZS_DEBUG ("cci_send() failed\n");
		return CCI_ERROR;
	}

	return CCI_SUCCESS;
}

static int
send_block_done(comm_state_t *cs, request_t *req)
{
	int ret = 0, offset = 0;
	uint32_t send_len = 0;
	int conn_index;
	zs_block_t *b = container_of(req, zs_block_t, req);
	zs_file_t *f = b->file;
	char buf[32], block[192];

	assert(cs->zs->role == ZS_SINK);

	memset(block, 0, sizeof(block));
	ret = sprint_block(b, block, sizeof(block), &offset);
/*
#ifdef DEBUG
	if (!ret)
		fprintf(stderr, "%s: comm %d: %s: %s\n", zs_role(cs->zs),
				cs->id, __func__, block);
#endif
*/
	pack_done(buf, sizeof(buf), f->src_fd, b->file_offset,
			b->src_offset, &send_len);

	assert(send_len <= sizeof(buf));
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	conn_index = get_conn_index (cs);
	if (conn_index == -1)
		return CCI_ERROR;
	ret = cci_rma(cs->connections[conn_index], buf, send_len,
			cs->local, b->sink_offset,
			&cs->remote, b->src_offset,
			b->len, req, CCI_FLAG_READ);

/*#ifdef DEBUG
	if (ret) {
		fprintf(stderr, "%s: comm: %s: %s ** failed - need to requeue\n",
				zs_role(cs->zs), __func__, block);
	}
#endif*/

	return ret;
}

static int
send_file_done(comm_state_t *cs, request_t *req)
{
	char buf[16];
	uint32_t send_len = 0;
	int conn_index;
	zs_file_t *done = container_of(req, zs_file_t, req);

	assert(cs->zs->role == ZS_SRC);

	pack_file_close(buf, sizeof(buf), done->sink_fd, &send_len);

	assert(send_len <= sizeof(buf));
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm: %s\n", zs_role(cs->zs), __func__);
#endif
*/
	conn_index = get_conn_index (cs);
	if (conn_index == -1)
		return CCI_ERROR;
	return cci_send(cs->connections[conn_index], buf, send_len, req, 0);
}

static int
send_bye(comm_state_t *cs)
{
	int ret = 0;
	char buf[16];
	uint32_t send_len = 0;
	int conn_index;

	if (cs->done)
		return 0;

	pack_bye(buf, sizeof(buf), &send_len);

	ZS_DEBUG ("%s: comm: %s\n", zs_role(cs->zs), __func__);

	conn_index = get_conn_index (cs);
	if (conn_index == -1) {
		ZS_DEBUG ("Cannot find valid connection\n");
		return CCI_ERROR;
	}
	ret = cci_send(cs->connections[conn_index], buf, send_len, NULL, CCI_FLAG_BLOCKING);
	if (ret == CCI_SUCCESS)
		cs->done++;

	return ret;
}

static void
progress_comm_requests(comm_state_t *cs)
{
	int ret = 0;
	zs_globals_t *zs = cs->zs;
	pthread_mutex_t *lock = &zs->comm[cs->id]->lock;
	request_t *req = NULL;

	if (!cs->ready)
		return;

	pthread_mutex_lock(lock);
	if (TAILQ_EMPTY(&zs->comm[cs->id]->q)) {
		pthread_mutex_unlock(lock);
		goto out;
	}
	req = TAILQ_FIRST(&zs->comm[cs->id]->q);
	TAILQ_REMOVE(&zs->comm[cs->id]->q, req, entry);
	pthread_mutex_unlock(lock);
/*
#ifdef DEBUG
	fprintf(stderr, "%s: comm %d: %s: processing %s (%p)\n", zs_role(cs->zs),
			cs->id, __func__, request_type_str(req->type),
			(void*) req);
#endif
*/
	switch (req->type) {
	case REQ_NEW_FILE:
		ret = send_new_file(cs, req);
		break;
	case REQ_FILE_ID:
		ret = send_file_id(cs, req);
		break;
	case REQ_NEW_BLOCK:
		ret = send_new_block(cs, req);
		break;
	case REQ_BLOCK_DONE:
		ret = send_block_done(cs, req);
		break;
	case REQ_FILE_DONE:
		ret = send_file_done(cs, req);
		break;
	case REQ_BYE:
		ret = send_bye(cs);
		break;
	default:
/*#ifdef DEBUG
		fprintf(stderr, "%s: ignoring request %s (%p)\n", __func__,
				request_type_str(req->type), (void*)req);
#endif*/
		assert(1);
	}

	if (ret) {
		pthread_mutex_lock(lock);
		TAILQ_INSERT_HEAD(&zs->comm[cs->id]->q, req, entry);
		pthread_mutex_unlock(lock);
	}

 out:

	return;
}

void
comm(void *args)
{
	zs_globals_t *zs = (zs_globals_t *)args;
	comm_state_t *cs = NULL;
	int ret = 0;
	int i = 0;
	int flags = CCI_FLAG_READ | CCI_FLAG_WRITE;
	uint32_t caps = 0;
	uint64_t len = (uint64_t)zs->rma_mtu * (uint64_t)zs->rma_cnt;
	char *uri = NULL;
	pthread_t tid = pthread_self();

	assert (zs);

	cs = calloc(1, sizeof(*cs));
	if (!cs) {
#ifdef DEBUG
		fprintf(stderr, "%s: no memory for comm\n", __func__);
#endif
		goto out;
	}

	cs->zs = zs;
	/* Initialize the array of connections */
	cs->connections = calloc (zs->num_conns, sizeof (cci_connection_t*));

	TAILQ_INIT(&zs->comm[cs->id]->q);

	/* determine who we are */
	cs->id = -1;

	for (i = 0; i < zs->num_comms; i++) {
		if (pthread_equal(tid, zs->comm[i]->tid))
			cs->id = i;
	}

	assert(cs->id != -1);
	zs->comm[cs->id]->priv = cs;

	ret = cci_init(CCI_ABI_VERSION, 0, &caps);
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_init() failed with %s\n", __func__,
				cci_strerror(NULL, ret));
#endif
		goto out;
	}

	ret = cci_create_endpoint(NULL, 0, &cs->endpoint, NULL);
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_create_endpoint() failed with %s\n", __func__,
				cci_strerror(NULL, ret));
#endif
		goto out_w_init;
	}

	ret = posix_memalign(&zs->rma_buf, sysconf(_SC_PAGESIZE), len);
  numa_tonode_memory(zs->rma_buf, len, 0);

	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: posix_memalign(rma_buf) failed with %s\n",
				__func__, strerror(ret));
#endif
		goto out_w_ep;
	}

#ifdef DEBUG
	fprintf(stderr, "%s: rma_buf = %p\n", __func__, zs->rma_buf);
#endif

	memset(zs->rma_buf, 0, len);

	ret = cci_rma_register(cs->endpoint, zs->rma_buf, len, flags, &cs->local);
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_rma_register() failed with %s\n", __func__,
				cci_strerror(cs->endpoint, ret));
#endif
		goto out_w_buf;
	}

	ret = cci_get_opt(cs->endpoint, CCI_OPT_ENDPT_URI, &uri);
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_create_endpoint() failed with %s\n", __func__,
				cci_strerror(cs->endpoint, ret));
#endif
		goto out_w_reg;
	}

	fprintf(stderr, "%s: opened %s\n", __func__, uri);

  ////// For SciFS
  FILE *fp_uri;
  fp_uri = fopen("/var/scifs/lads/src/uri", "w");  //// TODO : move the uri file path to config part
  fprintf(fp_uri, "%s", uri);
  fclose(fp_uri);

	free(uri);

	/* establish connection */
	ret = open_connections(cs);
	if (ret)
		goto out_w_reg;

	ZS_DEBUG ("%s: ready\n", __func__);

	/* wait for all threads to start */
	pthread_mutex_lock(zs->lock);
	if (!zs->done)
		zs->ready++;
	pthread_mutex_unlock(zs->lock);

	if (zs->done)
		goto out_w_reg;

	while (!cs->done && !zs->done) {
		progress_comm(cs);
		progress_comm_requests(cs);
	}

	ret = send_bye(cs);
	if (ret != CCI_SUCCESS)
		ZS_DEBUG ("send_bye() failed\n");
	sleep (1); /* XXX Quick work around to make sure the msg go through, we should poll on event until we get an ACK */

 out_w_reg:
	cci_rma_deregister(cs->endpoint, cs->local);

 out_w_buf:
	free(zs->rma_buf);

 out_w_ep:
	ret = cci_destroy_endpoint(cs->endpoint);
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_destroy_endpoint() failed with %s\n", __func__,
				cci_strerror(NULL, ret));
#endif
		goto out_w_init;
	}

 out_w_init:
	ret = cci_finalize();
	if (ret) {
#ifdef DEBUG
		fprintf(stderr, "%s: cci_finalize() failed with %s\n", __func__,
				cci_strerror(NULL, ret));
#endif
		goto out;
	}
 out:
	ZS_DEBUG ("%s: shutting down\n", __func__);

	free(cs);

	pthread_mutex_lock(zs->lock);
	zs->done++;
	pthread_mutex_unlock(zs->lock);

	pthread_exit(NULL);
}
