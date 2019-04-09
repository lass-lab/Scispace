#ifndef FD_TREE_H
#define FD_TREE_H

#include <stdint.h>
#include <errno.h>
#include <pthread.h>

/* The binary tree actually a sorted array that performs a binary search over
 * the array.
 *
 * For this implementation, the array will hold pointers to fd_node_t.
 */

/* This struct holds the fd and the associated pointer. I use a void *
 * rather that the specific type to avoid having to know what is stored.
 * You can use the pointer to the actual data struct instead of the void *.
 */
typedef struct fd_node {
	int fd;
	void *context;
} fd_node_t;

/* This struct holds the array, the number of array elements, and a lock. */
typedef struct fd_tree {
	fd_node_t **nodes;
	int count;
	pthread_mutex_t lock;
} fd_tree_t;

/* Init the fd_tree.
 *
 * \param[out] ftreep: A pointer to a fd_tree_t *.
 *
 * \return 0        Success
 * \return ENOMEM   Not enough memory
 * \return EINVAL   Invalid parameter (ftree == NULL?)
 * \return EAGAIN   The system temporarily lacks resources
 * \return EPERM    The user lacks privileges to call pthread_mutex_init()
 *
 * Must be called before any other functions can be used.
 */
int
fd_tree_init(fd_tree_t **ftreep);

/* Free all resources.
 *
 * \param[in] ftree: The pointer to the fd_tree_t returned from fd_tree_init().
 *
 * \return 0        Success
 * \return EINVAL   Invalid parameter (ftree == NULL?)
 *
 * Must be called before any other functions can be used.
 */
int
fd_tree_destroy(fd_tree_t *ftree);

/* Add fd and context to fd_tree.
 *
 * \param[in] ftree:   The pointer to the fd_tree_t.
 * \param[in] fd:      The file descriptor to add.
 * \param[in] context: The context associated with fd.
 *
 * \return 0        Success.
 * \return ENOMEM   Not enough memory.
 * \return EINVAL   Invalid parameter.
 * \return EBUSY    Attempting to cleanup while holding lock.
 *
 * This operation adds a new fd_node_t with fd and context to the fd_tree.
 * It sorts the tree.
 */
int
fd_tree_add(fd_tree_t *ftree, int fd, void *context);

/* Remove fd and context to fd_tree.
 *
 * \param[in] ftree:   The pointer to the fd_tree_t.
 * \param[in] fd:      The file descriptor to remove.
 * \param[in] context: The context associated with fd.
 *
 * \return 0        Success.
 * \return EINVAL   Invalid parameter.
 *
 * This operation removes the fd_node_t with fd and context from the fd_tree.
 * It sorts the tree.
 *
 * It can fail if ftree is NULL, fd does not exist in the fd_tree, or if the context
 * does not match the fd.
 */
int
fd_tree_remove(fd_tree_t *ftree, int fd, void *context);

/* Return the context for the given fd.
 *
 * \param[in] ftree:    The pointer to the fd_tree_t.
 * \param[in] fd:       The file descriptor to add.
 * \param[out] context: The context associated with fd.
 *
 * \return 0        Success.
 * \return EINVAL   Invalid parameter.
 *
 * Find the fd_node_t given the fd and return the associated context.
 *
 * It can fail is ftree is NULL or fd is not in the fd_tree_t.
 */
int
fd_tree_find(fd_tree_t *ftree, int fd, void **contextp);

void
fd_tree_print(fd_tree_t *ftree);

#endif /* FD_TREE_H */
