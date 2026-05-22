/*
 * Amalgame Standard Library — Amalgame.Async
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * Cooperative concurrency: Fiber + Channel + Scheduler + I/O.
 *
 * Stackful coroutines built on POSIX ucontext (Linux + macOS +
 * *BSD). Single-threaded round-robin scheduler with an epoll
 * (Linux) integration so socket / pipe reads park the fiber
 * instead of blocking the OS thread. Composes with
 * amalgame-threading for CPU parallelism (one scheduler per OS
 * thread; M:N is a v0.4 concern).
 *
 * v0.2 surface (single class `Async`, all methods static):
 *
 *   ── Fiber ──────────────────────────────────────────
 *   AmalgameFiber* FiberSpawn(closure, arg)
 *   void           FiberYield()
 *   void           FiberSleep(i64 ms)
 *   i64            FiberCurrentId()
 *
 *   ── Channel ────────────────────────────────────────
 *   AmalgameAsyncChannel* ChannelNew(capacity)
 *   code_bool        ChannelSend(ch, value)     parks if full
 *   i64              ChannelReceive(ch)         parks if empty
 *   code_bool        ChannelTrySend(ch, value)
 *   i64              ChannelTryReceive(ch)
 *   void             ChannelClose(ch)
 *   code_bool        ChannelIsClosed(ch)
 *   i64              ChannelCount(ch)
 *   i64              ChannelCapacity(ch)
 *
 *   ── Scheduler ──────────────────────────────────────
 *   void             SchedulerRun()
 *   void             SchedulerRunUntil(i64 ms)
 *   i64              SchedulerPending()
 *
 *   ── I/O (v0.2, Linux only) ─────────────────────────
 *   code_bool        WaitFdReadable(i64 fd, i64 timeout_ms)
 *   code_bool        WaitFdWritable(i64 fd, i64 timeout_ms)
 *   code_bool        MakeNonBlocking(i64 fd)
 *
 *      WaitFd* parks the current fiber until the kernel reports
 *      the fd is ready (epoll EPOLLIN/EPOLLOUT) or until
 *      timeout_ms milliseconds elapse. Negative timeout = wait
 *      forever. Returns 1 on readiness, 0 on timeout or error.
 *      MUST be called from inside a fiber; from main thread it
 *      returns 0 immediately (use blocking syscalls there).
 *      v0.2.0 supports one waiter per fd at a time — if two
 *      fibers wait on the same fd concurrently, the second
 *      overwrites the first registration and only one wakes.
 *      Future: per-fd waiter lists, plus kqueue/IOCP backends.
 *
 *      MakeNonBlocking sets O_NONBLOCK on the fd via fcntl —
 *      mandatory for the WaitFd dance to work correctly. Pair
 *      `MakeNonBlocking(fd)` + a loop of `WaitFd*` + `read/write`
 *      with EAGAIN handling.
 *
 * GC integration: each fiber owns a GC_memalign'd stack block.
 * That keeps parked fibers' locals scannable (the stack memory
 * is itself a GC object, scanned conservatively by libgc as part
 * of marking the fiber struct's children). When the running
 * fiber is on its own stack, the scheduler updates the
 * collector's stack bottom via GC_set_stackbottom so a
 * collection triggered mid-fiber finds the right roots. On
 * yield, we restore the original (main thread) stack bottom.
 *
 * Closure model: same as amalgame-threading — `FiberSpawn` takes
 * a 1-arg `AmalgameClosure*` (`(arg: int) => int`). The closure
 * captures enclosing locals by value at creation time. Mutate
 * shared state through `Channel` (no Mutex needed; scheduler is
 * single-threaded so reads/writes between yields are atomic).
 *
 * Out of scope (v0.2):
 *   - kqueue (BSD + macOS) I/O backend (v0.2.1 — today's WaitFd
 *     is Linux-only, emits a compile-time warning + returns 0
 *     on other platforms)
 *   - IOCP (Windows) I/O backend (v0.3, after the Windows
 *     Fibers backend lands)
 *   - Per-fd multi-fiber wait lists (one waiter per fd today)
 *   - Timer wheel for >1k concurrent sleepers (v0.3 — today
 *     the sleep list is sorted insertion in O(N))
 *   - `Async.Select` multi-channel readiness (v0.3)
 */

#ifndef AMALGAME_ASYNC_H
#define AMALGAME_ASYNC_H

#include "_runtime.h"

#if !(defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__) \
      || defined(__OpenBSD__) || defined(__NetBSD__) || defined(__unix__))
# error "amalgame-async v0.1 requires POSIX ucontext. Windows backend (Fibers API) is planned for v0.2."
#endif

#include <ucontext.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#ifdef __linux__
# include <sys/epoll.h>
# define AMASYNC_HAS_EPOLL 1
#else
# define AMASYNC_HAS_EPOLL 0
#endif

/* ─── bdwgc stack-bottom switching ────────────────────
 * `_runtime.h` pulls in <gc.h>, which declares struct
 * GC_stack_base + the two functions below. They're stable since
 * bdwgc 7.6 (2015) and shipped by every distro we target. */

/* ═══════════════════════════════════════════════════════
 *  Fiber state + scheduler globals
 * ═══════════════════════════════════════════════════════ */

typedef enum {
    AMASYNC_READY    = 0,
    AMASYNC_RUNNING  = 1,
    AMASYNC_SLEEPING = 2,
    AMASYNC_WAITING  = 3,  /* parked on a channel queue */
    AMASYNC_DEAD     = 4
} AmalgameFiberState;

typedef struct AmalgameFiber {
    ucontext_t            ctx;
    void*                 stack;        /* GC_memalign'd block */
    size_t                stack_size;
    AmalgameClosure*      fn;
    void*                 arg;
    i64                   id;
    int                   state;
    i64                   wake_at_ms;   /* set when SLEEPING or when fd-wait has a timeout */
    int                   fd_waiting;       /* -1 if not waiting on an fd; otherwise the fd */
    i64                   fd_deadline_ms;   /* -1 if no timeout; otherwise wake_at_ms copy */
    int                   fd_wakeup_reason; /* set on wake — 1 = ready, 0 = timeout/error */
    struct AmalgameFiber* next;         /* queue link */
} AmalgameFiber;

typedef struct AmalgameAsyncScheduler {
    AmalgameFiber* ready_head;
    AmalgameFiber* ready_tail;
    AmalgameFiber* sleeping;            /* sorted ascending wake_at_ms */
    AmalgameFiber* current;
    i64            waiting_count;       /* fibers parked on channels OR fds */
    i64            fd_waiters_count;    /* subset of waiting_count parked on fds */
    int            epoll_fd;            /* epoll instance — see epoll_initialized */
    int            epoll_initialized;   /* 0 until first WaitFd call */
    ucontext_t     main_ctx;
    struct GC_stack_base main_sb;
    int            main_sb_captured;
    i64            next_id;
    int            running;
} AmalgameAsyncScheduler;

static AmalgameAsyncScheduler _amasync_sched;

/* ═══════════════════════════════════════════════════════
 *  Internal helpers
 * ═══════════════════════════════════════════════════════ */

static inline i64 _amasync_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (i64) ts.tv_sec * 1000 + (i64) (ts.tv_nsec / 1000000);
}

static inline void _amasync_ready_push(AmalgameFiber* f) {
    f->next = NULL;
    if (_amasync_sched.ready_tail) {
        _amasync_sched.ready_tail->next = f;
        _amasync_sched.ready_tail = f;
    } else {
        _amasync_sched.ready_head = _amasync_sched.ready_tail = f;
    }
}

static inline AmalgameFiber* _amasync_ready_pop(void) {
    AmalgameFiber* f = _amasync_sched.ready_head;
    if (!f) return NULL;
    _amasync_sched.ready_head = f->next;
    if (!_amasync_sched.ready_head) _amasync_sched.ready_tail = NULL;
    f->next = NULL;
    return f;
}

static inline void _amasync_sleep_insert(AmalgameFiber* f) {
    AmalgameFiber** cur = &_amasync_sched.sleeping;
    while (*cur && (*cur)->wake_at_ms <= f->wake_at_ms) {
        cur = &(*cur)->next;
    }
    f->next = *cur;
    *cur = f;
}

static inline void _amasync_sleep_remove(AmalgameFiber* f) {
    AmalgameFiber** cur = &_amasync_sched.sleeping;
    while (*cur) {
        if (*cur == f) {
            *cur = f->next;
            f->next = NULL;
            return;
        }
        cur = &(*cur)->next;
    }
}

static inline void _amasync_wake_due_sleepers(i64 now) {
    while (_amasync_sched.sleeping
            && _amasync_sched.sleeping->wake_at_ms <= now) {
        AmalgameFiber* f = _amasync_sched.sleeping;
        _amasync_sched.sleeping = f->next;
        f->next = NULL;
        f->state = AMASYNC_READY;
        if (f->fd_waiting >= 0) {
            /* Was a timed fd-wait — timeout fired before any event.
             * Remove the fd from epoll so a later readiness event
             * doesn't dispatch to a stale fiber pointer. */
#if AMASYNC_HAS_EPOLL
            if (_amasync_sched.epoll_initialized) {
                epoll_ctl(_amasync_sched.epoll_fd, EPOLL_CTL_DEL,
                          f->fd_waiting, NULL);
            }
#endif
            f->fd_wakeup_reason = 0;  /* timeout */
            f->fd_waiting = -1;
            f->fd_deadline_ms = -1;
            if (_amasync_sched.fd_waiters_count > 0) _amasync_sched.fd_waiters_count--;
            if (_amasync_sched.waiting_count > 0)    _amasync_sched.waiting_count--;
        }
        _amasync_ready_push(f);
    }
}

static inline void _amasync_queue_push(AmalgameFiber** head, AmalgameFiber* f) {
    f->next = NULL;
    if (!*head) { *head = f; return; }
    AmalgameFiber* p = *head;
    while (p->next) p = p->next;
    p->next = f;
}

static inline AmalgameFiber* _amasync_queue_pop(AmalgameFiber** head) {
    AmalgameFiber* f = *head;
    if (!f) return NULL;
    *head = f->next;
    f->next = NULL;
    return f;
}

/* GC stackbottom shuffling. `GC_set_stackbottom(NULL, sb)` updates
 * the calling thread's registered stack bottom — used to redirect
 * the conservative scan to the active fiber's stack while it runs,
 * and back to the original main stack on yield. */
static inline void _amasync_use_fiber_stackbottom(AmalgameFiber* f) {
    struct GC_stack_base sb;
    sb.mem_base = (char*) f->stack + f->stack_size;
    GC_set_stackbottom(NULL, &sb);
}

static inline void _amasync_use_main_stackbottom(void) {
    GC_set_stackbottom(NULL, &_amasync_sched.main_sb);
}

/* ═══════════════════════════════════════════════════════
 *  Fiber entry trampoline
 * ═══════════════════════════════════════════════════════ */

/* makecontext() can only pass int arguments portably. We dodge
 * that by reading _amasync_sched.current — which the scheduler
 * sets right before swapping into us — instead of receiving the
 * fiber pointer as an argument. */
static void _amasync_fiber_entry(void) {
    AmalgameFiber* f = _amasync_sched.current;
    if (f && f->fn) {
        AmalgameClosure_call1(f->fn, f->arg);
    }
    if (f) f->state = AMASYNC_DEAD;
    /* Hand control back to the scheduler. No need to update the
     * stackbottom here: setcontext() switches the OS stack
     * pointer, and the scheduler will fix up the bottom right
     * after the swap returns. */
    setcontext(&_amasync_sched.main_ctx);
    /* unreachable */
}

/* ═══════════════════════════════════════════════════════
 *  Public API
 * ═══════════════════════════════════════════════════════ */

static inline AmalgameFiber* Amalgame_Async_FiberSpawn(AmalgameClosure* fn, i64 arg) {
    AmalgameFiber* f = (AmalgameFiber*) GC_MALLOC(sizeof(AmalgameFiber));
    f->stack_size = 64 * 1024;
    /* GC_MALLOC returns a pointer-scanned block so locals on
     * the fiber stack stay reachable through this single GC
     * root. Pointer-aligned (8 bytes on x86_64) is enough —
     * makecontext aligns the stack pointer to 16 bytes
     * internally on SysV-ABI targets. */
    f->stack = GC_MALLOC(f->stack_size);
    f->fn = fn;
    f->arg = (void*) (intptr_t) arg;
    f->id = ++_amasync_sched.next_id;
    f->state = AMASYNC_READY;
    f->wake_at_ms = 0;
    f->fd_waiting = -1;
    f->fd_deadline_ms = -1;
    f->fd_wakeup_reason = 0;
    f->next = NULL;

    getcontext(&f->ctx);
    f->ctx.uc_stack.ss_sp   = f->stack;
    f->ctx.uc_stack.ss_size = f->stack_size;
    f->ctx.uc_stack.ss_flags = 0;
    f->ctx.uc_link = &_amasync_sched.main_ctx;
    makecontext(&f->ctx, _amasync_fiber_entry, 0);

    _amasync_ready_push(f);
    return f;
}

/* Yield from inside a fiber back to the scheduler. Caller has
 * already set the fiber's state and (re)queued it. */
static inline void _amasync_yield_to_main(AmalgameFiber* f) {
    _amasync_use_main_stackbottom();
    swapcontext(&f->ctx, &_amasync_sched.main_ctx);
    /* Resumed here on next time-slice. Set fiber stackbottom
     * back so a GC triggered while we run sees this stack. */
    _amasync_use_fiber_stackbottom(f);
}

static inline void Amalgame_Async_FiberYield(void) {
    AmalgameFiber* f = _amasync_sched.current;
    if (!f) return;  /* not in a scheduler — no-op */
    f->state = AMASYNC_READY;
    _amasync_ready_push(f);
    _amasync_yield_to_main(f);
}

static inline void Amalgame_Async_FiberSleep(i64 ms) {
    AmalgameFiber* f = _amasync_sched.current;
    if (!f) {
        /* Outside any fiber — fall back to OS-level sleep so the
         * call is still meaningful from `Main()`. */
        if (ms <= 0) return;
        struct timespec ts;
        ts.tv_sec  = (time_t) (ms / 1000);
        ts.tv_nsec = (long)   ((ms % 1000) * 1000000L);
        nanosleep(&ts, NULL);
        return;
    }
    if (ms <= 0) ms = 0;
    f->state = AMASYNC_SLEEPING;
    f->wake_at_ms = _amasync_now_ms() + ms;
    _amasync_sleep_insert(f);
    _amasync_yield_to_main(f);
}

static inline i64 Amalgame_Async_FiberCurrentId(void) {
    return _amasync_sched.current ? _amasync_sched.current->id : 0;
}

/* ═══════════════════════════════════════════════════════
 *  I/O — epoll (Linux) integration
 * ═══════════════════════════════════════════════════════ */

#if AMASYNC_HAS_EPOLL

static inline void _amasync_init_epoll(void) {
    if (_amasync_sched.epoll_initialized) return;
    _amasync_sched.epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    /* If epoll_create1 fails (very unlikely on Linux ≥ 2.6.27),
     * leave epoll_fd at whatever -1 / error value epoll returned.
     * Subsequent epoll_ctl calls will then fail and WaitFd will
     * return 0 — degraded but not catastrophic. */
    _amasync_sched.epoll_initialized = 1;
}

/* Common waiter for EPOLLIN / EPOLLOUT. Returns 1 on ready,
 * 0 on timeout or error / not-in-fiber. */
static inline code_bool _amasync_wait_fd(i64 fd, uint32_t events_mask, i64 timeout_ms) {
    AmalgameFiber* f = _amasync_sched.current;
    if (!f) return 0;  /* only meaningful inside a fiber */

    _amasync_init_epoll();
    if (_amasync_sched.epoll_fd < 0) return 0;

    struct epoll_event ev;
    ev.events = events_mask | EPOLLONESHOT;
    ev.data.ptr = f;

    if (epoll_ctl(_amasync_sched.epoll_fd, EPOLL_CTL_ADD, (int) fd, &ev) < 0) {
        if (errno == EEXIST) {
            /* fd was registered for a prior Wait — rearm via MOD */
            if (epoll_ctl(_amasync_sched.epoll_fd, EPOLL_CTL_MOD, (int) fd, &ev) < 0) {
                return 0;
            }
        } else {
            return 0;
        }
    }

    f->state = AMASYNC_WAITING;
    f->fd_waiting = (int) fd;
    f->fd_wakeup_reason = 0;
    if (timeout_ms >= 0) {
        f->fd_deadline_ms = _amasync_now_ms() + timeout_ms;
        f->wake_at_ms = f->fd_deadline_ms;
        _amasync_sleep_insert(f);
    } else {
        f->fd_deadline_ms = -1;
    }
    _amasync_sched.waiting_count++;
    _amasync_sched.fd_waiters_count++;
    _amasync_yield_to_main(f);

    int reason = f->fd_wakeup_reason;
    f->fd_waiting = -1;
    f->fd_deadline_ms = -1;
    f->fd_wakeup_reason = 0;
    return reason > 0 ? 1 : 0;
}

static inline code_bool Amalgame_Async_WaitFdReadable(i64 fd, i64 timeout_ms) {
    return _amasync_wait_fd(fd, EPOLLIN, timeout_ms);
}

static inline code_bool Amalgame_Async_WaitFdWritable(i64 fd, i64 timeout_ms) {
    return _amasync_wait_fd(fd, EPOLLOUT, timeout_ms);
}

#else  /* !AMASYNC_HAS_EPOLL */
# warning "amalgame-async v0.2: WaitFd* is Linux-only (epoll). kqueue/IOCP backends planned for v0.2.1+ — these calls return 0 on this platform."

static inline code_bool Amalgame_Async_WaitFdReadable(i64 fd, i64 timeout_ms) {
    (void) fd; (void) timeout_ms;
    return 0;
}
static inline code_bool Amalgame_Async_WaitFdWritable(i64 fd, i64 timeout_ms) {
    (void) fd; (void) timeout_ms;
    return 0;
}

#endif /* AMASYNC_HAS_EPOLL */

static inline code_bool Amalgame_Async_MakeNonBlocking(i64 fd) {
    int flags = fcntl((int) fd, F_GETFL, 0);
    if (flags < 0) return 0;
    if (fcntl((int) fd, F_SETFL, flags | O_NONBLOCK) < 0) return 0;
    return 1;
}

/* ═══════════════════════════════════════════════════════
 *  Channel — scheduler-aware bounded FIFO
 * ═══════════════════════════════════════════════════════ */

typedef struct AmalgameAsyncChannel {
    void**           buffer;
    i64              capacity;
    i64              count;
    i64              head;
    i64              tail;
    int              closed;
    AmalgameFiber*   waiters_send;
    AmalgameFiber*   waiters_recv;
} AmalgameAsyncChannel;

static inline AmalgameAsyncChannel* Amalgame_Async_ChannelNew(i64 capacity) {
    if (capacity <= 0) capacity = 1;
    AmalgameAsyncChannel* ch =
        (AmalgameAsyncChannel*) GC_MALLOC(sizeof(AmalgameAsyncChannel));
    ch->buffer = (void**) GC_MALLOC(sizeof(void*) * (size_t) capacity);
    ch->capacity = capacity;
    ch->count = 0;
    ch->head = 0;
    ch->tail = 0;
    ch->closed = 0;
    ch->waiters_send = NULL;
    ch->waiters_recv = NULL;
    return ch;
}

static inline void _amasync_chan_wake_one(AmalgameFiber** head) {
    AmalgameFiber* f = _amasync_queue_pop(head);
    if (!f) return;
    f->state = AMASYNC_READY;
    _amasync_ready_push(f);
    if (_amasync_sched.waiting_count > 0) _amasync_sched.waiting_count--;
}

static inline code_bool Amalgame_Async_ChannelSend(AmalgameAsyncChannel* ch, i64 value) {
    if (!ch) return 0;
    while (1) {
        if (ch->closed) return 0;
        if (ch->count < ch->capacity) {
            ch->buffer[ch->tail] = (void*) (intptr_t) value;
            ch->tail = (ch->tail + 1) % ch->capacity;
            ch->count++;
            _amasync_chan_wake_one(&ch->waiters_recv);
            return 1;
        }
        /* full — park the fiber on the send queue */
        AmalgameFiber* f = _amasync_sched.current;
        if (!f) {
            /* Outside a fiber AND channel full: refuse rather
             * than loop forever. Callers from non-fiber context
             * should use TrySend. */
            return 0;
        }
        f->state = AMASYNC_WAITING;
        _amasync_queue_push(&ch->waiters_send, f);
        _amasync_sched.waiting_count++;
        _amasync_yield_to_main(f);
        /* loop — re-check capacity on resume */
    }
}

static inline code_bool Amalgame_Async_ChannelTrySend(AmalgameAsyncChannel* ch, i64 value) {
    if (!ch || ch->closed || ch->count == ch->capacity) return 0;
    ch->buffer[ch->tail] = (void*) (intptr_t) value;
    ch->tail = (ch->tail + 1) % ch->capacity;
    ch->count++;
    _amasync_chan_wake_one(&ch->waiters_recv);
    return 1;
}

static inline i64 Amalgame_Async_ChannelReceive(AmalgameAsyncChannel* ch) {
    if (!ch) return 0;
    while (1) {
        if (ch->count > 0) {
            i64 v = (i64) (intptr_t) ch->buffer[ch->head];
            ch->head = (ch->head + 1) % ch->capacity;
            ch->count--;
            _amasync_chan_wake_one(&ch->waiters_send);
            return v;
        }
        if (ch->closed) return 0;  /* drained + closed → sentinel */
        AmalgameFiber* f = _amasync_sched.current;
        if (!f) return 0;  /* outside a fiber + empty: don't block */
        f->state = AMASYNC_WAITING;
        _amasync_queue_push(&ch->waiters_recv, f);
        _amasync_sched.waiting_count++;
        _amasync_yield_to_main(f);
    }
}

static inline i64 Amalgame_Async_ChannelTryReceive(AmalgameAsyncChannel* ch) {
    if (!ch || ch->count == 0) return 0;
    i64 v = (i64) (intptr_t) ch->buffer[ch->head];
    ch->head = (ch->head + 1) % ch->capacity;
    ch->count--;
    _amasync_chan_wake_one(&ch->waiters_send);
    return v;
}

static inline void Amalgame_Async_ChannelClose(AmalgameAsyncChannel* ch) {
    if (!ch) return;
    ch->closed = 1;
    /* Wake everyone — senders observe `closed` and return 0,
     * receivers drain remaining buffer then return 0 sentinel. */
    while (ch->waiters_send) _amasync_chan_wake_one(&ch->waiters_send);
    while (ch->waiters_recv) _amasync_chan_wake_one(&ch->waiters_recv);
}

static inline code_bool Amalgame_Async_ChannelIsClosed(AmalgameAsyncChannel* ch) {
    return (ch && ch->closed) ? 1 : 0;
}

static inline i64 Amalgame_Async_ChannelCount(AmalgameAsyncChannel* ch) {
    return ch ? ch->count : 0;
}

static inline i64 Amalgame_Async_ChannelCapacity(AmalgameAsyncChannel* ch) {
    return ch ? ch->capacity : 0;
}

/* ═══════════════════════════════════════════════════════
 *  Scheduler
 * ═══════════════════════════════════════════════════════ */

/* deadline_ms < 0 means run forever (until everything completes).
 * Otherwise stop once the wall clock passes the deadline. */
static inline void _amasync_scheduler_pump(i64 deadline_ms) {
    if (!_amasync_sched.main_sb_captured) {
        GC_get_my_stackbottom(&_amasync_sched.main_sb);
        _amasync_sched.main_sb_captured = 1;
    }
    _amasync_sched.running = 1;

    while (1) {
        i64 now = _amasync_now_ms();
        _amasync_wake_due_sleepers(now);

        if (deadline_ms >= 0 && now >= deadline_ms) break;

        AmalgameFiber* f = _amasync_ready_pop();
        if (!f) {
            /* No runnable fibers. Three potential wakeup sources:
             *   1. A sleeper's wake_at_ms passes  → time-driven
             *   2. epoll signals a watched fd     → I/O-driven
             *   3. The RunUntil deadline elapses  → caller's clock
             * If none of {sleepers, fd-waiters} exist, only
             * channel waiters remain — that's a deadlock; bail.
             */
            i64 next_wake = -1;
            if (_amasync_sched.sleeping) next_wake = _amasync_sched.sleeping->wake_at_ms;
            if (deadline_ms >= 0 && (next_wake < 0 || deadline_ms < next_wake)) {
                next_wake = deadline_ms;
            }
            int has_fd_waiters = _amasync_sched.fd_waiters_count > 0;

            if (next_wake < 0 && !has_fd_waiters) {
                /* No time-driven wakeup. If channel waiters remain
                 * (waiting_count > fd_waiters_count == 0), only
                 * external pokes could unstick them — bail. */
                break;
            }

            int timeout_ms = -1;
            if (next_wake >= 0) {
                i64 dt = next_wake - now;
                timeout_ms = dt > 0 ? (int) dt : 0;
            }

#if AMASYNC_HAS_EPOLL
            if (has_fd_waiters && _amasync_sched.epoll_initialized) {
                struct epoll_event evs[16];
                int n = epoll_wait(_amasync_sched.epoll_fd, evs, 16, timeout_ms);
                for (int i = 0; i < n; i++) {
                    AmalgameFiber* w = (AmalgameFiber*) evs[i].data.ptr;
                    if (!w || w->state != AMASYNC_WAITING || w->fd_waiting < 0) continue;
                    if (w->fd_deadline_ms >= 0) _amasync_sleep_remove(w);
                    w->fd_wakeup_reason = 1;
                    w->state = AMASYNC_READY;
                    if (_amasync_sched.fd_waiters_count > 0) _amasync_sched.fd_waiters_count--;
                    if (_amasync_sched.waiting_count > 0)    _amasync_sched.waiting_count--;
                    _amasync_ready_push(w);
                }
                continue;
            }
#endif
            /* No fd waiters — fall back to plain nanosleep. */
            if (timeout_ms > 0) {
                struct timespec ts;
                ts.tv_sec  = (time_t) (timeout_ms / 1000);
                ts.tv_nsec = (long)   ((timeout_ms % 1000) * 1000000L);
                nanosleep(&ts, NULL);
            }
            continue;
        }

        /* Run the fiber. Update GC stack bottom so any collection
         * triggered by allocation inside the fiber scans the
         * right region. */
        _amasync_use_fiber_stackbottom(f);
        _amasync_sched.current = f;
        f->state = AMASYNC_RUNNING;
        swapcontext(&_amasync_sched.main_ctx, &f->ctx);
        _amasync_sched.current = NULL;
        /* Back in scheduler-loop — restore main stackbottom. */
        _amasync_use_main_stackbottom();

        if (f->state == AMASYNC_DEAD) {
            /* Drop the closure + stack refs so libgc can collect
             * them once no external pointer survives. */
            f->fn = NULL;
            f->stack = NULL;
        }
        /* Otherwise the fiber re-parked itself in ready /
         * sleeping / waiting before yielding. */
    }

    _amasync_sched.running = 0;
}

static inline void Amalgame_Async_SchedulerRun(void) {
    _amasync_scheduler_pump(-1);
}

static inline void Amalgame_Async_SchedulerRunUntil(i64 ms) {
    if (ms < 0) ms = 0;
    _amasync_scheduler_pump(_amasync_now_ms() + ms);
}

static inline i64 Amalgame_Async_SchedulerPending(void) {
    i64 n = 0;
    AmalgameFiber* p;
    for (p = _amasync_sched.ready_head; p; p = p->next) n++;
    for (p = _amasync_sched.sleeping;   p; p = p->next) n++;
    n += _amasync_sched.waiting_count;
    return n;
}

#endif /* AMALGAME_ASYNC_H */
