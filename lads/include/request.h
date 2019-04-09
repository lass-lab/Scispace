#ifndef REQUEST_H
#define REQUEST_H

#include <sys/queue.h>

typedef enum request_type {
	REQ_INVALID = 0,
	REQ_NEW_FILE,
	REQ_FILE_ID,
	REQ_NEW_BLOCK,
	REQ_BLOCK_DONE,
	REQ_FILE_DONE,
	REQ_BYE
} request_type_t;

static inline char *
request_type_str(request_type_t type)
{
	char *str = NULL;

	switch (type) {
	case REQ_INVALID:
		str = "REQ_INVALID";
		break;
	case REQ_NEW_FILE:
		str = "REQ_NEW_FILE";
		break;
	case REQ_FILE_ID:
		str = "REQ_FILE_ID";
		break;
	case REQ_NEW_BLOCK:
		str = "REQ_NEW_BLOCK";
		break;
	case REQ_BLOCK_DONE:
		str = "REQ_BLOCK_DONE";
		break;
	case REQ_FILE_DONE:
		str = "REQ_FILE_DONE";
		break;
	case REQ_BYE:
		str = "REQ_BYE";
		break;
	}
	return str;
}

typedef struct request {
	request_type_t type;
	TAILQ_ENTRY (request) entry;
} request_t;

#endif /* REQUEST_H */
