#ifndef MUTEX_H
#define MUTEX_H

#ifdef _WIN32
#include <windows.h>
#include <process.h>
typedef CRITICAL_SECTION MUTEX_T;
typedef SRWLOCK RWLOCK_T;
#define MUTEX_INIT(p) InitializeCriticalSection((p))
#define MUTEX_LOCK(p) EnterCriticalSection((p))
#define MUTEX_UNLOCK(p) LeaveCriticalSection((p))
#define MUTEX_DESTROY(p) DeleteCriticalSection((p))
#define RWLOCK_INIT(p) InitializeSRWLock((p))
#define RWLOCK_RDLOCK(p) AcquireSRWLockShared((p))
#define RWLOCK_WRLOCK(p) AcquireSRWLockExclusive((p))
#define RWLOCK_UNLOCK(p) ReleaseSRWLockShared((p))
#define RWLOCK_UNLOCK_W(p) ReleaseSRWLockExclusive((p))
#define RWLOCK_DESTROY(p) ((void)(p))
#else
#include <pthread.h>
typedef pthread_mutex_t MUTEX_T;
typedef pthread_rwlock_t RWLOCK_T;
#define MUTEX_INIT(p) pthread_mutex_init((p), NULL)
#define MUTEX_LOCK(p) pthread_mutex_lock((p))
#define MUTEX_UNLOCK(p) pthread_mutex_unlock((p))
#define MUTEX_DESTROY(p) pthread_mutex_destroy((p))
#define RWLOCK_INIT(p) pthread_rwlock_init((p), NULL)
#define RWLOCK_RDLOCK(p) pthread_rwlock_rdlock((p))
#define RWLOCK_WRLOCK(p) pthread_rwlock_wrlock((p))
#define RWLOCK_UNLOCK(p) pthread_rwlock_unlock((p))
#define RWLOCK_UNLOCK_W(p) pthread_rwlock_unlock((p))
#define RWLOCK_DESTROY(p) pthread_rwlock_destroy((p))
#endif

#endif