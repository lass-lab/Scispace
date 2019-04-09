#include <string.h>
#include <arpa/inet.h>
#include <errno.h>

#ifndef CCI_H
typedef const struct cci_rma_handle {
	uint64_t stuff[4];
} cci_rma_handle_t;
#endif

typedef enum msg_type {
	CONNECT = 0,	/* Block size, file list size, RMA handle */
	READY,		/* Have FILE_LIST, start sending NEW_FILE */
	NEW_FILE,	/* Starting new file, need FILE_ID */
	FILE_ID,	/* Here is FILE_ID, start sending NEW_BLOCKS */
	NEW_BLOCK,	/* Block is ready for RMA Read */
	BLOCK_DONE,	/* RMA Read is done, recycle this block */
	BYE,		/* ready to disconnect */
	FILE_CLOSE	/* file close*/
} msg_type_t;

typedef union header {
	msg_type_t type;	/* always first in each msg */

	/* Use this struct to determine the length of the header */
	struct msg_connect_size {
		msg_type_t type;	/* CONNECT */
		uint32_t blocksize;	/* our preferred block size */
		uint32_t num_blocks;	/* number of blocksized blocks */
		uint32_t list_len;	/* length of the file list */
	} connect_size;

	/* Use this struct to access elements and point to the payload */
	/* I did not embed the cci_rma_handle_t in the header because it
	 * is uint64_t aligned and would throw off the alignment.
	 */
	struct msg_connect {
		msg_type_t type;	/* CONNECT */
		uint32_t blocksize;	/* our preferred block size */
		uint32_t num_blocks;	/* number of blocksized blocks */
		uint32_t list_len;	/* length of the file list */
		char data[1];		/* Start of the RMA handle */
	} connect;

	/* No payload, so no need for a data pointer */
	struct msg_ready {
		msg_type_t type;	/* READY */
	} ready;

	/* Use this struct to determine the length of the header */
	struct msg_new_file_size {
		msg_type_t type;	/* NEW_FILE */
		uint32_t name_len;	/* Length of file name */
		uint32_t file_len[2];	/* Length of file */
		uint32_t src_fd;	/* Src's file id */
	} new_file_size;

	/* Use this struct to access elements and point to the file name */
	struct msg_new_file {
		msg_type_t type;	/* NEW_FILE */
		uint32_t name_len;	/* Length of file name (including NULL) */
#define HI (0)
#define LO (1)
		uint32_t file_len[2];	/* Length of file (uint64_t stored in unit32_t */
		uint32_t src_fd;	/* Src's file id */
		char data[1];		/* Start of file name */
	} new_file;

	/* No payload, so no need for a data pointer */
	struct msg_file_id {
		msg_type_t type;	/* NEW_BLOCK */
		uint32_t src_fd;	/* Src's file id */
		uint32_t dst_fd;	/* Dst's file id */
	} file_id;

	/* No payload, so no need for a data pointer */
	struct msg_new_block {
		msg_type_t type;	/* NEW_BLOCK */
		uint32_t dst_fd;	/* Dst's file id */
		uint32_t file_offset[2]; /* Offset in file */
		uint32_t src_offset[2];	/* Offset in RMA buffer */
		uint32_t len;		/* Length of block */
	} block;

	/* No payload, so no need for a data pointer */
	struct msg_block_done {
		msg_type_t type;	/* BLOCK_DONE */
		uint32_t src_fd;	/* Src's file id */
		uint32_t file_offset[2]; /* Offset in file */
		uint32_t src_offset[2]; /* Src RMA offset in file */
	} done;

	struct msg_bye {
		msg_type_t type;	/* BYE */
	} bye;

	struct msg_file_close{
		msg_type_t type;
		uint32_t sink_id;
	} close;
} hdr_t;

static inline int
pack_connect(void *buf, uint32_t buf_len, uint32_t blocksize, uint32_t num_blocks,
		uint32_t list_len, cci_rma_handle_t *handle, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = 0;

	need = sizeof(hdr->connect_size) + sizeof(*handle);
	if (buf_len < need)
		return EINVAL;

	hdr->connect.type = htonl(CONNECT);
	hdr->connect.blocksize = htonl(blocksize);
	hdr->connect.num_blocks = htonl(num_blocks);
	hdr->connect.list_len = htonl(list_len);
	memcpy(hdr->connect.data, handle, sizeof(*handle));

	*send_len = need;

	return 0;
}

static inline int
parse_connect(void *buf, uint32_t buf_len, uint32_t *blocksize, uint32_t *num_blocks,
		uint32_t *list_len, cci_rma_handle_t *handle)
{
	hdr_t *hdr = buf;
	uint32_t need = 0;

	need = sizeof(hdr->connect_size) + sizeof(*handle);
	if (buf_len < need)
		return EINVAL;

	*blocksize = ntohl(hdr->connect.blocksize);
	*num_blocks = ntohl(hdr->connect.num_blocks);
	*list_len = ntohl(hdr->connect.list_len);
	memcpy((void*)handle, hdr->connect.data, sizeof(*handle));

	return 0;
}

/* After receiving the CONNECT, the sink will need to RMA Read the list of
 * files from RMA buffer 0. The files need to be packed such that it is
 * encoded filename length followed by the filename.
 *
 * After retrieving the file list, send READY message.
 */


static inline int
pack_ready(void *buf, uint32_t buf_len)
{
	hdr_t *hdr = buf;

	if (buf_len < sizeof(hdr->ready))
		return EINVAL;

	hdr->ready.type = htonl(READY);
	return 0;
}

/* There is no parse ready function because there is nothing but the message type */

static inline int
pack_new_file(void *buf, uint32_t buf_len, uint32_t name_len,
		uint64_t file_len, uint32_t src_fd, char *name, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = 0;

	need = sizeof(hdr->new_file_size) + name_len + 1; /* NULL terminator */
	if (buf_len < need)
		return EINVAL;

	hdr->new_file.type = htonl(NEW_FILE);
	hdr->new_file.name_len = htonl(name_len);
	hdr->new_file.file_len[HI] = htonl((uint32_t)(file_len >> 32));
	hdr->new_file.file_len[LO] = htonl((uint32_t)(file_len));
	hdr->new_file.src_fd = htonl(src_fd);
	strncpy(hdr->new_file.data, name, name_len + 1);
	*send_len = need;

	return 0;
}

static inline int
parse_new_file(void *buf, uint32_t buf_len, uint32_t *name_len, uint64_t *file_len, uint32_t *src_fd, char **name)
{
	hdr_t *hdr = buf;
	uint32_t need = 0;
	uint64_t len = 0;
	char *n = NULL;

	/* Do we have a valid NEW_FILE header? */
	need = sizeof(hdr->new_file_size);
	if (buf_len < need)
		return EINVAL;

	/* Do we have the complete file name? */
	*name_len = ntohl(hdr->new_file.name_len);
	need += *name_len;
	if (buf_len < need)
		return EINVAL;

	n = calloc(1, *name_len + 1);
	if (!n)
		return ENOMEM;

	strncpy(n, hdr->new_file.data, *name_len);

	len = ((uint64_t) ntohl(hdr->new_file.file_len[HI])) << 32;
	len |= (uint64_t) ntohl(hdr->new_file.file_len[LO]);
	*file_len = len;
	*src_fd = ntohl(hdr->new_file.src_fd);
	*name = n;

	return 0;
}

static inline int
pack_file_id(void *buf, uint32_t buf_len, uint32_t src_fd, uint32_t dst_fd, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = sizeof(hdr->file_id);

	if (buf_len < need)
		return EINVAL;

	hdr->file_id.type = htonl(FILE_ID);
	hdr->file_id.src_fd = htonl(src_fd);
	hdr->file_id.dst_fd = htonl(dst_fd);
	*send_len = need;

	return 0;
}

static inline int
parse_file_id(void *buf, uint32_t buf_len, uint32_t *src_fd, uint32_t *dst_fd)
{
	hdr_t *hdr = buf;
	uint32_t need = sizeof(hdr->file_id);

	if (buf_len < need)
		return EINVAL;

	*src_fd = ntohl(hdr->file_id.src_fd);
	*dst_fd = ntohl(hdr->file_id.dst_fd);

	return 0;
}

static inline int
pack_block(void *buf, uint32_t buf_len, uint32_t dst_fd, uint64_t file_offset,
		uint64_t src_offset, uint32_t len, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = sizeof(hdr->block);

	if (buf_len < need)
		return EINVAL;

	hdr->block.type = htonl(NEW_BLOCK);
	hdr->block.dst_fd = htonl(dst_fd);
	hdr->block.file_offset[HI] = htonl((uint32_t)(file_offset >> 32));
	hdr->block.file_offset[LO] = htonl((uint32_t)file_offset);
	hdr->block.src_offset[HI] = htonl((uint32_t)(src_offset >> 32));
	hdr->block.src_offset[LO] = htonl((uint32_t)src_offset);
	hdr->block.len = htonl(len);
	*send_len = need;

	return 0;
}

static inline int
parse_block(void *buf, uint32_t buf_len, uint32_t *dst_fd, uint64_t *file_offset,
		uint64_t *src_offset, uint32_t *len)
{
	hdr_t *hdr = buf;
	uint64_t f_off = 0, src_off;
	uint32_t need = sizeof(hdr->block);

	if (buf_len < need)
		return EINVAL;

	*dst_fd = ntohl(hdr->block.dst_fd);
	f_off = ((uint64_t) ntohl(hdr->block.file_offset[HI])) << 32;
	f_off |= (uint64_t) ntohl(hdr->block.file_offset[LO]);
	*file_offset = f_off;
	src_off = ((uint64_t) ntohl(hdr->block.src_offset[HI])) << 32;
	src_off |= (uint64_t) ntohl(hdr->block.src_offset[LO]);
	*src_offset = src_off;
	*len = ntohl(hdr->block.len);

	return 0;
}

static inline int
pack_done(void *buf, uint32_t buf_len, uint32_t src_fd, uint64_t file_offset, uint64_t src_offset, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = sizeof(hdr->done);
  //fprintf(stderr, "(pack_done) need size : %d\n", need);

	if (buf_len < need)
		return EINVAL;

	hdr->done.type = htonl(BLOCK_DONE);
	hdr->done.src_fd = htonl(src_fd);
	hdr->done.file_offset[HI] = htonl((uint32_t)(file_offset >> 32));
	hdr->done.file_offset[LO] = htonl((uint32_t)file_offset);
	hdr->done.src_offset[HI] = htonl((uint32_t)(src_offset >> 32));
	hdr->done.src_offset[LO] = htonl((uint32_t)src_offset);
	*send_len = need;

	return 0;
}

static inline int
parse_done(void *buf, uint32_t buf_len, uint32_t *src_fd, uint64_t *file_offset,
		uint64_t *src_offset)
{
	hdr_t *hdr = buf;
	uint64_t f_off = 0, src_off = 0;
	uint32_t need = sizeof(hdr->done);

	if (buf_len < need)
		return EINVAL;

	*src_fd = ntohl(hdr->done.src_fd);
	f_off = ((uint64_t)(ntohl(hdr->done.file_offset[HI]))) << 32;
	f_off |= (uint64_t)(ntohl(hdr->done.file_offset[LO]));
	*file_offset = f_off;
	src_off = ((uint64_t)(ntohl(hdr->done.src_offset[HI]))) << 32;
	src_off |= (uint64_t)(ntohl(hdr->done.src_offset[LO]));
	*src_offset = src_off;

	return 0;
}

static inline int
pack_file_close(void *buf, uint32_t buf_len, uint32_t sink_fd, uint32_t *send_len)
{
	hdr_t *hdr = buf;
	uint32_t need = sizeof(hdr->close);

	if (buf_len < sizeof(hdr->close))
		return EINVAL;

	hdr->close.type = htonl(FILE_CLOSE);
	hdr->close.sink_id = htonl(sink_fd);
	*send_len = need;
	return 0;
}

static inline int
pack_bye(void *buf, uint32_t buf_len, uint32_t *send_len)
{
	hdr_t *hdr = buf;

	if (buf_len < sizeof(hdr->bye))
		return EINVAL;

	hdr->bye.type = htonl(BYE);
	*send_len = sizeof(hdr->bye);
	return 0;
}
