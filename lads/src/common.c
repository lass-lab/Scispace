#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>

#include "zs.h"

/* Format a buffer with the block's information
 *
 * \param[in] b:	Pointer to block
 * \param[in] buf:	Pointer to buffer to pack into
 * \param[in] buf_len:	Length of buffer
 * \param[out] offset:	Length of stored buffer info
 *
 * Return 0 on success, else -1 on truncation, -2 all other errors
 */
int
sprint_block(zs_block_t *b, char *buf, int buf_len, int *offset)
{
	int ret = 0, off = 0;

	ret = snprintf(buf, buf_len, "block %p:\n\tost %p len %u file %s\n "
			"\tfile_offset %"PRIu64" src_offset %"PRIu64" sink_offset "
			"%"PRIu64"\n\t",
			(void*) b, (void*)b->ost, b->len, b->file->name, b->file_offset,
			b->src_offset, b->sink_offset);
	if (ret > 0) {
		if (ret < buf_len) {
			off = ret;
			ret = 0;
		} else {
			/* truncation */
			ret = -1;
		}
	} else {
		ret = -2;
	}

	if (!ret)
		*offset = off;

	return ret;
}

int
print_block(zs_block_t *b)
{
	int ret = 0, offset = 0;
	char buf[1024];

	ret = sprint_block(b, buf, sizeof(buf), &offset);
	if (!ret)
		fprintf(stderr, "%s\n", buf);

	return ret;
}
