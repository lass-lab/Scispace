#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#include "fd_tree.h"

int
fd_tree_init(fd_tree_t **ftreep)
{
	int ret = 0;
	fd_tree_t *f = NULL;

	if (!ftreep) {
		ret = EINVAL;
		goto out;
	}

	f = calloc(1, sizeof(*f));
	if (!f) {
		ret = ENOMEM;
		goto out;
	}

	ret = pthread_mutex_init(&f->lock, NULL);
	if (ret)
		goto out;

	*ftreep = f;

    out:
	return ret;
}


int
fd_tree_destroy(fd_tree_t *ftree)
{
	int ret = 0;

	if (!ftree) {
		ret = EINVAL;
		goto out;
	}

	ret = pthread_mutex_destroy(&ftree->lock);
	if (ret)
		goto out;

	free(ftree->nodes);
	free(ftree);

    out:
	return ret;
}

int
compare_nodes(const void *np1, const void *np2)
{
	fd_node_t *n1 = *((fd_node_t**)np1);
	fd_node_t *n2 = *((fd_node_t**)np2);

	return n1->fd > n2->fd ? 1 : n1->fd < n2->fd ? -1 : 0;
}

static int
find_node_locked(fd_tree_t *ftree, int fd, void *context, fd_node_t **nodep)
{
	int ret = 0;
	fd_node_t **node = NULL, tmp, *key = &tmp;

	tmp.fd = fd;

	node = bsearch(&key, ftree->nodes, ftree->count, sizeof(*node), compare_nodes);
	if (node) {
		if (!context || ((*node)->context == context)) {
			*nodep = *node;
		} else {
			ret = EINVAL;
		}
	} else {
		*nodep = NULL;
		ret = ENOENT;
	}

	return ret;
}

int
fd_tree_add(fd_tree_t *ftree, int fd, void *context)
{
	int ret = 0;
	fd_node_t *node = NULL, **nodes = NULL;

	pthread_mutex_lock(&ftree->lock);

	/* check if the fd already exists, it should not */
	ret = find_node_locked(ftree, fd, context, &node);
	if (ret != ENOENT) {
		if (ret == 0)
			ret = EEXIST;
		goto out;
	}

	/* alloc the new node */
	node = calloc(1, sizeof(*node));
	if (!node) {
		ret = ENOMEM;
		goto out;
	}

	node->fd = fd;
	node->context = context;

	/* increment the array count */
	ftree->count++;

	/* try to alloc a new array and copy the existing array over */
	nodes = realloc(ftree->nodes, ftree->count * sizeof(node));
	if (!nodes) {
		ftree->count--;
		ret = ENOMEM;
		goto out;
	}

	ret = 0;
	ftree->nodes = nodes;
	ftree->nodes[ftree->count - 1] = node;

	qsort(ftree->nodes, ftree->count, sizeof(node), compare_nodes);

    out:
	pthread_mutex_unlock(&ftree->lock);

	if (ret)
		free(node);

	return ret;
}

int
fd_tree_remove(fd_tree_t *ftree, int fd, void *context)
{
	int ret = 0;
	fd_node_t *node = NULL, **nodes = NULL;

	pthread_mutex_lock(&ftree->lock);

	/* find the node */
	ret = find_node_locked(ftree, fd, context, &node);
	if (ret)
		goto out;

	/* set the fd to the largest value possible... */
	node->fd = INT_MAX;

	/* and sort the array to move this item to the end. */
	qsort(ftree->nodes, ftree->count, sizeof(node), compare_nodes);

	/* Decrease the array count */
	ftree->count--;

	/* try to alloc a new array and copy the existing array over */
	nodes = realloc(ftree->nodes, ftree->count * sizeof(node));
	if (nodes || ftree->count == 0) {
		/* If realloc() fails, we still have the old, larger
		 * array but count will ensure that we ignore the last,
		 * unwanted node pointer.
		 */
		ftree->nodes = nodes;
	}

	free(node);

    out:
	pthread_mutex_unlock(&ftree->lock);

	return ret;
}

int
fd_tree_find(fd_tree_t *ftree, int fd, void **contextp)
{
	int ret = 0;
	fd_node_t *node = NULL;

	pthread_mutex_lock(&ftree->lock);

	ret = find_node_locked(ftree, fd, NULL, &node);
	if (ret)
		goto out;

	*contextp = node->context;

    out:
	pthread_mutex_unlock(&ftree->lock);

	return ret;
}

void
fd_tree_print(fd_tree_t *ftree)
{
	int i = 0;

	pthread_mutex_lock(&ftree->lock);

	for (i = 0; i < ftree->count; i++) {
		fd_node_t *node = ftree->nodes[i];

		printf("fd %3d context %p\n", node->fd, node->context);
	}

	pthread_mutex_unlock(&ftree->lock);

	return;
}
