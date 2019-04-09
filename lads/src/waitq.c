#include "waitq.h"

void
waitq_init(waitq_t *q, pthread_mutex_t *lock)
{
	q->lock = lock;
	TAILQ_INIT(&q->waiters);

	return;
}

void
waitq_wait_locked(waitq_t *q, waiter_t *w)
{
	TAILQ_INSERT_TAIL(&q->waiters, w, entry);
	pthread_cond_wait(&w->cv, q->lock);

	return;
}

void
waitq_wait(waitq_t *q, waiter_t *w)
{
	pthread_mutex_lock(q->lock);
	waitq_wait_locked(q, w);
	pthread_mutex_unlock(q->lock);

	return;
}

void
waitq_signal(waitq_t *q)
{
	waiter_t *w = NULL;

	pthread_mutex_lock(q->lock);
	w = TAILQ_FIRST(&q->waiters);
	if (w) {
		TAILQ_REMOVE(&q->waiters, w, entry);
		pthread_cond_signal(&w->cv);
	}
	pthread_mutex_unlock(q->lock);

	return;
}

void
waitq_broadcast(waitq_t *q)
{
	waiter_t *w = NULL;

	pthread_mutex_lock(q->lock);
	while ((w = TAILQ_FIRST(&q->waiters)) != NULL) {
		TAILQ_REMOVE(&q->waiters, w, entry);
		pthread_cond_signal(&w->cv);
	}
	pthread_mutex_unlock(q->lock);

	return;
}
