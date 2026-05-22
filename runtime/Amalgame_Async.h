/*
 * Amalgame Standard Library — Amalgame.Async
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * Cooperative concurrency: Fiber + Channel + Scheduler.
 *
 * Stackful coroutines built on POSIX ucontext (Linux + macOS +
 * *BSD). Single-threaded round-robin scheduler. Composes with
 * amalgame-threading for CPU parallelism (one scheduler per OS
 * thread; M:N is a v0.4 concern).
 *
 * v0.1 surface (single class `Async`, all methods static):
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
 * Out of scope (v0.1):
 *   - epoll / kqueue / IOCP I/O integration (v0.2)
 *   - Windows Fibers backend (v0.2 — MinGW lacks ucontext)
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
    i64                   wake_at_ms;   /* set when state == SLEEPING */
    struct AmalgameFiber* next;         /* queue link */
} AmalgameFiber;

typedef struct AmalgameAsyncScheduler {
    AmalgameFiber* ready_head;
    AmalgameFiber* ready_tail;
    AmalgameFiber* sleeping;            /* sorted ascending wake_at_ms */
    AmalgameFiber* current;
    i64            waiting_count;       /* fibers parked on channels */
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

static inline void _amasync_wake_due_sleepers(i64 now) {
    while (_amasync_sched.sleeping
            && _amasync_sched.sleeping->wake_at_ms <= now) {
        AmalgameFiber* f = _amasync_sched.sleeping;
        _amasync_sched.sleeping = f->next;
        f->next = NULL;
        f->state = AMASYNC_READY;
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
            /* No runnable fibers. If sleepers exist, OS-sleep
             * until the earliest wake_at (or the deadline). If
             * nothing is sleeping and nothing is waiting on a
             * channel, the scheduler is done. */
            if (!_amasync_sched.sleeping) {
                if (_amasync_sched.waiting_count == 0) break;
                /* Channel-parked fibers with no possible wakeup
                 * source = deadlock. Bail to avoid spinning. */
                break;
            }
            i64 wake = _amasync_sched.sleeping->wake_at_ms;
            if (deadline_ms >= 0 && deadline_ms < wake) wake = deadline_ms;
            i64 dt = wake - now;
            if (dt > 0) {
                struct timespec ts;
                ts.tv_sec  = (time_t) (dt / 1000);
                ts.tv_nsec = (long)   ((dt % 1000) * 1000000L);
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
