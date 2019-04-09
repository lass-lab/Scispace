#ifndef WAITQ_H
#define WAITQ_H

#include <pthread.h>
#include <sys/queue.h>

typedef struct waitq  waitq_t;
typedef struct waiter waiter_t;

struct waitq {
	pthread_mutex_t *lock;
	TAILQ_HEAD(w, waiter) waiters;
};

struct waiter {
	TAILQ_ENTRY(waiter) entry;
	pthread_cond_t cv;
};

void waitq_init(waitq_t *q, pthread_mutex_t *lock);
void waitq_wait_locked(waitq_t *q, waiter_t *w);
void waitq_wait(waitq_t *q, waiter_t *w);
void waitq_signal(waitq_t *q);
void waitq_broadcast(waitq_t *q);

#endif /* WAITQ_H */
