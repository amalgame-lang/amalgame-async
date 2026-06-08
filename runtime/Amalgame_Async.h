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
 *
 * Shipped since:
 *   - v0.3.0: `Async.Select` multi-channel readiness —
 *     SelectReceive / SelectTryReceive / SelectValue. A fiber
 *     registers a waiter node on every channel at once (not the
 *     naive "spawn N racers" hack), so no value is consumed-then-
 *     discarded; round-robin start offset keeps it starvation-free.
 */

#ifndef AMALGAME_ASYNC_H
#define AMALGAME_ASYNC_H

#include "_runtime.h"

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef _WIN32
  /* Windows backend: POSIX ucontext doesn't exist, but the Win32 Fibers
   * API is a near-exact match (cooperative, stackful, explicit switch).
   * We shim ucontext_t + get/make/swap/setcontext onto fibers so the
   * scheduler below compiles and runs unchanged. WaitFd* is already a
   * no-op stub off the epoll path (see the !AMASYNC_HAS_EPOLL branch),
   * so no IOCP/WSAPoll loop is needed for this backend. */
  #ifndef WIN32_LEAN_AND_MEAN
  #  define WIN32_LEAN_AND_MEAN
  #endif
  #include <winsock2.h>
  #include <windows.h>

  typedef struct amasync_ucontext {
      void* fiber;                 /* Win32 fiber handle */
      struct { void* ss_sp; size_t ss_size; int ss_flags; } uc_stack;
      struct amasync_ucontext* uc_link;
      void (*_entry)(void);
  } amasync_ucontext_t;
  #define ucontext_t amasync_ucontext_t

  static inline void* amasync_self_fiber(void) {
      if (!IsThreadAFiber()) return ConvertThreadToFiber(NULL);
      return GetCurrentFiber();
  }
  static inline int amasync_getcontext(ucontext_t* c) {
      memset(c, 0, sizeof(*c));
      return 0;
  }
  static void __stdcall amasync_fiber_trampoline(void* p) {
      ((ucontext_t*) p)->_entry();
  }
  static inline void amasync_makecontext(ucontext_t* c, void (*fn)(void), int argc) {
      (void) argc;
      c->_entry = fn;
      size_t sz = c->uc_stack.ss_size ? c->uc_stack.ss_size : (size_t)(64 * 1024);
      c->fiber  = CreateFiber(sz, amasync_fiber_trampoline, c);
  }
  static inline int amasync_swapcontext(ucontext_t* from, ucontext_t* to) {
      from->fiber = amasync_self_fiber();
      SwitchToFiber(to->fiber);
      return 0;
  }
  static inline void amasync_setcontext(ucontext_t* to) {
      SwitchToFiber(to->fiber);
  }
  #define getcontext(c)        amasync_getcontext(c)
  #define makecontext(c,fn,n)  amasync_makecontext((c),(fn),(n))
  #define swapcontext(f,t)     amasync_swapcontext((f),(t))
  #define setcontext(t)        amasync_setcontext(t)
#else
  #include <ucontext.h>
  #include <fcntl.h>
  #include <unistd.h>
#endif

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
    /* v0.2.2: cancellation.
     *   `cancelled` flips to 1 when Async.FiberCancel(f) is called.
     *   `chan_wait_head` points at the channel queue head that holds
     *      this fiber (so cancellation can splice it out without
     *      scanning every channel). NULL when not channel-waiting. */
    int                   cancelled;
    struct AmalgameFiber** chan_wait_head;
    /* v0.3: Async.Select multi-channel receive.
     *   select_parked = 1 while the fiber is parked across N channels'
     *     select_recv lists. chan_wait_head stays NULL — the waiter
     *     nodes live on the fiber's own stack and are unlinked on
     *     resume, so a single fiber can sit on N channels at once
     *     (f->next can only thread one plain wait queue).
     *   select_value caches the value pulled by the winning channel,
     *     read back via Async.SelectValue(). */
    int                   select_parked;
    i64                   select_value;
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
    i64            select_rr;           /* round-robin start offset — Select fairness */
    i64            select_value;        /* SelectValue() fallback when called from main */
    int            running;
} AmalgameAsyncScheduler;

/* v0.2.1: must NOT be `static` — when multiple translation units
 * include this header (consumer + several packages), `static` gives
 * each TU its own copy of the scheduler, breaking cross-TU fiber
 * accounting. `weak` linkage tells the linker to merge them into
 * one instance, header-only-friendly. */
__attribute__((weak)) AmalgameAsyncScheduler _amasync_sched;

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
    f->cancelled = 0;
    f->chan_wait_head = NULL;
    f->select_parked = 0;
    f->select_value = 0;
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
    if (f->cancelled) return;  /* v0.2.2: don't park if already cancelled */
    if (ms <= 0) ms = 0;
    f->state = AMASYNC_SLEEPING;
    f->wake_at_ms = _amasync_now_ms() + ms;
    _amasync_sleep_insert(f);
    _amasync_yield_to_main(f);
    /* Resumed: cancelled fibers may have been woken early by
     * Amalgame_Async_FiberCancel; that's an indistinguishable
     * normal-wake from this side. Callers detect via IsCancelled. */
}

static inline i64 Amalgame_Async_FiberCurrentId(void) {
    return _amasync_sched.current ? _amasync_sched.current->id : 0;
}

/* ═══════════════════════════════════════════════════════
 *  Cancellation (v0.2.2)
 * ═══════════════════════════════════════════════════════
 *
 * Cooperative cancellation. `Async.FiberCancel(f)`:
 *   - flips f->cancelled = 1
 *   - if f is parked (sleep / fd-wait / channel-wait), splices it
 *     out of that queue and pushes it to ready so it resumes
 *     promptly at its next yield point
 *
 * The cancelled fiber observes the wake exactly like a normal
 * resume — `Async.FiberSleep` returns, `WaitFd*` returns 0,
 * `ChannelSend/Receive` return 0 / false. Callers detect cancel
 * by calling `Async.IsCancelled()` at the yield point or by
 * treating those sentinel returns as cancel hints.
 *
 * Typical user pattern:
 *
 *     while (!Async.IsCancelled()) {
 *         let work: int = Async.ChannelReceive(queue)
 *         if (Async.IsCancelled()) { break }     // recv woke us via cancel, not work
 *         process(work)
 *     }
 *
 * Use cases:
 *   - In-flight graceful shutdown — cancel every per-connection
 *     fiber when SIGTERM arrives.
 *   - Request-scoped timeouts — spawn a worker fiber + a timer
 *     fiber that cancels the worker if it overstays.
 *   - "Wait for first of N events" — spawn N waiters, cancel the
 *     losers once one returns.
 */

static inline void Amalgame_Async_FiberCancel(AmalgameFiber* f) {
    if (!f || f->cancelled || f->state == AMASYNC_DEAD) return;
    f->cancelled = 1;

    /* Splice out of whatever queue f is parked on, set READY, push
     * to the run queue. Order matters: epoll first (so the fd is
     * cleaned before sleep_remove discards the deadline), sleep
     * next, channel last. */
    if (f->fd_waiting >= 0) {
#if AMASYNC_HAS_EPOLL
        if (_amasync_sched.epoll_initialized) {
            epoll_ctl(_amasync_sched.epoll_fd, EPOLL_CTL_DEL,
                      f->fd_waiting, NULL);
        }
#endif
        if (f->fd_deadline_ms >= 0) _amasync_sleep_remove(f);
        f->fd_waiting = -1;
        f->fd_deadline_ms = -1;
        f->fd_wakeup_reason = 0;  /* signals "not ready / cancelled" */
        if (_amasync_sched.fd_waiters_count > 0) _amasync_sched.fd_waiters_count--;
        if (_amasync_sched.waiting_count > 0)    _amasync_sched.waiting_count--;
        f->state = AMASYNC_READY;
        _amasync_ready_push(f);
        return;
    }
    if (f->state == AMASYNC_SLEEPING) {
        _amasync_sleep_remove(f);
        f->state = AMASYNC_READY;
        _amasync_ready_push(f);
        return;
    }
    if (f->state == AMASYNC_WAITING && f->chan_wait_head) {
        /* Walk the channel wait queue and splice f out. */
        AmalgameFiber** cur = f->chan_wait_head;
        while (*cur && *cur != f) cur = &(*cur)->next;
        if (*cur == f) {
            *cur = f->next;
            f->next = NULL;
        }
        f->chan_wait_head = NULL;
        if (_amasync_sched.waiting_count > 0) _amasync_sched.waiting_count--;
        f->state = AMASYNC_READY;
        _amasync_ready_push(f);
        return;
    }
    if (f->state == AMASYNC_WAITING && f->select_parked) {
        /* Parked in Async.Select across N channels. The waiter nodes
         * live on f's own stack and get unlinked by the select frame
         * when it resumes; here we only unpark + enqueue. A sender that
         * still sees a lingering node skips it (select_parked is 0 now),
         * and the resumed frame observes f->cancelled and bails. */
        f->select_parked = 0;
        if (_amasync_sched.waiting_count > 0) _amasync_sched.waiting_count--;
        f->state = AMASYNC_READY;
        _amasync_ready_push(f);
        return;
    }
    /* AMASYNC_READY or AMASYNC_RUNNING: nothing to splice — the
     * fiber will observe the flag at its next yield point. */
}

static inline code_bool Amalgame_Async_IsCancelled(void) {
    return (_amasync_sched.current && _amasync_sched.current->cancelled)
            ? 1 : 0;
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
    if (f->cancelled) return 0;  /* v0.2.2: don't park if already cancelled */

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
#ifdef _WIN32
    /* Winsock non-blocking switch — there's no fcntl on Windows, and
     * the fds the async layer parks on are sockets. */
    u_long mode = 1;
    if (ioctlsocket((SOCKET) fd, FIONBIO, &mode) != 0) return 0;
    return 1;
#else
    int flags = fcntl((int) fd, F_GETFL, 0);
    if (flags < 0) return 0;
    if (fcntl((int) fd, F_SETFL, flags | O_NONBLOCK) < 0) return 0;
    return 1;
#endif
}

/* ═══════════════════════════════════════════════════════
 *  Channel — scheduler-aware bounded FIFO
 * ═══════════════════════════════════════════════════════ */

/* v0.3: a fiber parked in Async.Select registers one of these nodes on
 * each watched channel's `select_recv` list. The node lives on the
 * selecting fiber's own (GC-scanned) stack; unlike waiters_send/recv it
 * does NOT reuse f->next, so one fiber can sit on N channels at once. */
typedef struct AmalgameSelectWaiter {
    AmalgameFiber*               fiber;
    struct AmalgameSelectWaiter* chan_next;
} AmalgameSelectWaiter;

typedef struct AmalgameAsyncChannel {
    void**                buffer;
    i64                   capacity;
    i64                   count;
    i64                   head;
    i64                   tail;
    int                   closed;
    AmalgameFiber*        waiters_send;
    AmalgameFiber*        waiters_recv;
    AmalgameSelectWaiter* select_recv;   /* v0.3: Async.Select waiters */
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
    ch->select_recv = NULL;
    return ch;
}

static inline void _amasync_chan_wake_one(AmalgameFiber** head) {
    AmalgameFiber* f = _amasync_queue_pop(head);
    if (!f) return;
    f->state = AMASYNC_READY;
    _amasync_ready_push(f);
    if (_amasync_sched.waiting_count > 0) _amasync_sched.waiting_count--;
}

/* v0.3: wake the first still-parked Select waiter on this channel.
 * Returns 1 if one was woken. Stale nodes (whose fiber was already
 * unparked via another channel) are skipped, not removed — the owning
 * select frame unlinks its own nodes when it resumes. */
static inline int _amasync_chan_wake_one_select(AmalgameAsyncChannel* ch) {
    AmalgameSelectWaiter* w = ch->select_recv;
    while (w) {
        AmalgameFiber* f = w->fiber;
        if (f && f->select_parked) {
            f->select_parked = 0;
            f->state = AMASYNC_READY;
            _amasync_ready_push(f);
            if (_amasync_sched.waiting_count > 0) _amasync_sched.waiting_count--;
            return 1;
        }
        w = w->chan_next;
    }
    return 0;
}

/* A value just became receivable on `ch`. Hand it to a committed plain
 * receiver if one is parked; otherwise nudge a Select waiter so it
 * re-scans and pulls the value. One value wakes at most one consumer. */
static inline void _amasync_chan_signal_recv(AmalgameAsyncChannel* ch) {
    if (ch->waiters_recv) { _amasync_chan_wake_one(&ch->waiters_recv); return; }
    _amasync_chan_wake_one_select(ch);
}

static inline code_bool Amalgame_Async_ChannelSend(AmalgameAsyncChannel* ch, i64 value) {
    if (!ch) return 0;
    while (1) {
        if (ch->closed) return 0;
        if (ch->count < ch->capacity) {
            ch->buffer[ch->tail] = (void*) (intptr_t) value;
            ch->tail = (ch->tail + 1) % ch->capacity;
            ch->count++;
            _amasync_chan_signal_recv(ch);
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
        if (f->cancelled) return 0;  /* v0.2.2: respect prior cancel */
        f->state = AMASYNC_WAITING;
        f->chan_wait_head = &ch->waiters_send;
        _amasync_queue_push(&ch->waiters_send, f);
        _amasync_sched.waiting_count++;
        _amasync_yield_to_main(f);
        f->chan_wait_head = NULL;
        if (f->cancelled) return 0;
        /* loop — re-check capacity on resume */
    }
}

static inline code_bool Amalgame_Async_ChannelTrySend(AmalgameAsyncChannel* ch, i64 value) {
    if (!ch || ch->closed || ch->count == ch->capacity) return 0;
    ch->buffer[ch->tail] = (void*) (intptr_t) value;
    ch->tail = (ch->tail + 1) % ch->capacity;
    ch->count++;
    _amasync_chan_signal_recv(ch);
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
        if (f->cancelled) return 0;  /* v0.2.2 */
        f->state = AMASYNC_WAITING;
        f->chan_wait_head = &ch->waiters_recv;
        _amasync_queue_push(&ch->waiters_recv, f);
        _amasync_sched.waiting_count++;
        _amasync_yield_to_main(f);
        f->chan_wait_head = NULL;
        if (f->cancelled) return 0;
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
    /* v0.3: wake every Select waiter so each re-scans and observes the
     * closed+empty sentinel. */
    while (_amasync_chan_wake_one_select(ch)) { }
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
 *  Select — wait for the first of N channels to be receivable
 *  (v0.3)
 * ═══════════════════════════════════════════════════════
 *
 * `Async.SelectReceive(channels)` parks the current fiber until any
 * channel in the list has a buffered value or is closed-and-drained,
 * then returns that channel's *index*; the received value is cached and
 * read back with `Async.SelectValue()` (call it immediately, before any
 * further yield/await). `SelectTryReceive` is the non-blocking form —
 * returns the index of an already-ready channel, or -1 if none.
 *
 *     let i = Async.SelectReceive(channels)   // blocks
 *     let v = Async.SelectValue()             // value from channels[i]
 *
 * Correctness: the fiber registers a waiter node on every channel at
 * once; a sender wakes at most one consumer per value; on resume the
 * fiber re-scans and pulls atomically — so no value is consumed-then-
 * discarded (the bug that sinks the naive "spawn N racer fibers"
 * approach). A round-robin start offset keeps it starvation-free.
 *
 * `channels` is a `List<Channel>`; the i64 return indexes into it. From
 * a closed+empty channel the value is the usual 0 sentinel (check with
 * `ch.IsClosed()` if 0 is a legal payload). Outside a fiber the call
 * never blocks: it returns a ready index or -1.
 */

static inline i64 _amasync_select_scan(AmalgameList* channels, i64 n, i64 start) {
    AmalgameFiber* f = _amasync_sched.current;
    for (i64 k = 0; k < n; k++) {
        i64 i = (start + k) % n;
        AmalgameAsyncChannel* ch =
            (AmalgameAsyncChannel*) AmalgameList_get(channels, (int) i);
        if (!ch) continue;
        if (ch->count > 0) {
            i64 v = (i64) (intptr_t) ch->buffer[ch->head];
            ch->head = (ch->head + 1) % ch->capacity;
            ch->count--;
            _amasync_chan_wake_one(&ch->waiters_send);  /* a blocked sender may proceed */
            if (f) f->select_value = v; else _amasync_sched.select_value = v;
            return i;
        }
        if (ch->closed) {
            if (f) f->select_value = 0; else _amasync_sched.select_value = 0;
            return i;  /* closed + empty → report it; value is the 0 sentinel */
        }
    }
    return -1;
}

static inline i64 Amalgame_Async_SelectTryReceive(AmalgameList* channels) {
    if (!channels) return -1;
    i64 n = AmalgameList_size(channels);
    if (n <= 0) return -1;
    i64 start = (_amasync_sched.select_rr++) % n;
    return _amasync_select_scan(channels, n, start);
}

static inline i64 Amalgame_Async_SelectReceive(AmalgameList* channels) {
    if (!channels) return -1;
    i64 n = AmalgameList_size(channels);
    if (n <= 0) return -1;
    AmalgameFiber* f = _amasync_sched.current;
    while (1) {
        i64 start = (_amasync_sched.select_rr++) % n;
        i64 hit = _amasync_select_scan(channels, n, start);
        if (hit >= 0) return hit;
        if (!f) return -1;          /* not in a fiber + nothing ready: don't block */
        if (f->cancelled) return -1;

        /* Register a waiter node on every channel, then park. The node
         * block is GC-managed and stays reachable via the channels'
         * select_recv chains (and this frame) for the park's duration. */
        AmalgameSelectWaiter* nodes = (AmalgameSelectWaiter*)
            GC_MALLOC(sizeof(AmalgameSelectWaiter) * (size_t) n);
        for (i64 i = 0; i < n; i++) {
            AmalgameAsyncChannel* ch =
                (AmalgameAsyncChannel*) AmalgameList_get(channels, (int) i);
            nodes[i].fiber = f;
            nodes[i].chan_next = NULL;
            if (ch) {
                nodes[i].chan_next = ch->select_recv;
                ch->select_recv = &nodes[i];
            }
        }
        f->select_parked = 1;
        f->state = AMASYNC_WAITING;
        _amasync_sched.waiting_count++;
        _amasync_yield_to_main(f);

        /* Resumed by a sender, a close, or a cancel. Unlink every node
         * from its channel before re-scanning so dangling stack nodes
         * never outlive the frame. */
        f->select_parked = 0;
        for (i64 i = 0; i < n; i++) {
            AmalgameAsyncChannel* ch =
                (AmalgameAsyncChannel*) AmalgameList_get(channels, (int) i);
            if (!ch) continue;
            AmalgameSelectWaiter** cur = &ch->select_recv;
            while (*cur && *cur != &nodes[i]) cur = &(*cur)->chan_next;
            if (*cur == &nodes[i]) *cur = nodes[i].chan_next;
        }
        if (f->cancelled) return -1;
        /* loop: a channel is (probably) ready now — re-scan and pull. */
    }
}

static inline i64 Amalgame_Async_SelectValue(void) {
    return _amasync_sched.current
        ? _amasync_sched.current->select_value
        : _amasync_sched.select_value;
}

/* ═══════════════════════════════════════════════════════
 *  WithTimeout (v0.2.3)
 * ═══════════════════════════════════════════════════════
 *
 * `Amalgame_Async_WithTimeout(closure, arg, ms)` runs `closure(arg)`
 * as a fresh fiber with a `ms`-millisecond budget. Returns 1 if the
 * closure finished before the deadline, 0 if the deadline fired
 * first (in which case the closure's fiber was FiberCancel'd —
 * it'll observe the wake at its next yield point and unwind).
 *
 * Two helper fibers race a 1-capacity Channel:
 *   - worker fiber: runs the user closure, then TrySend(1) on win.
 *   - timer fiber:  FiberSleep(ms), then TrySend(2) + FiberCancel
 *                   the worker on win.
 * The caller's fiber ChannelReceive's; whichever sender won decides
 * the return value. Both fibers are then cancelled (no-op on the
 * one that already finished). Idempotent.
 *
 * Composable: nest WithTimeout inside another WithTimeout — the
 * outer's cancel propagates to the inner via FiberCancel (the inner
 * worker observes IsCancelled at its next yield point).
 */

typedef struct {
    AmalgameClosure*      _wt_inner_fn;
    void*                 _wt_inner_arg;
    AmalgameAsyncChannel* _wt_done_ch;
} _amasync_wt_worker_env;

typedef struct {
    AmalgameFiber*        _wt_worker;
    i64                   _wt_ms;
    AmalgameAsyncChannel* _wt_done_ch;
} _amasync_wt_timer_env;

static void* _amasync_wt_worker_fn(void* envRaw, void* arg) {
    (void) arg;
    _amasync_wt_worker_env* e = (_amasync_wt_worker_env*) envRaw;
    AmalgameClosure_call1(e->_wt_inner_fn, e->_wt_inner_arg);
    /* TrySend: if the channel is already filled by the timer, we
     * silently lose the race — that's the "timeout fired" outcome
     * even though our closure happened to complete moments later. */
    Amalgame_Async_ChannelTrySend(e->_wt_done_ch, 1);
    return NULL;
}

static void* _amasync_wt_timer_fn(void* envRaw, void* arg) {
    (void) arg;
    _amasync_wt_timer_env* e = (_amasync_wt_timer_env*) envRaw;
    Amalgame_Async_FiberSleep(e->_wt_ms);
    if (Amalgame_Async_IsCancelled()) return NULL;
    /* Try to claim the timeout slot. If the worker already won,
     * TrySend fails and we don't cancel — the worker's value is
     * already in flight to the caller. */
    if (Amalgame_Async_ChannelTrySend(e->_wt_done_ch, 2)) {
        Amalgame_Async_FiberCancel(e->_wt_worker);
    }
    return NULL;
}

static inline code_bool Amalgame_Async_WithTimeout(
        AmalgameClosure* fn, i64 arg, i64 ms) {
    if (!fn) return 0;
    if (ms <= 0) return 0;  /* zero budget = instant timeout */

    AmalgameAsyncChannel* done = Amalgame_Async_ChannelNew(1);

    _amasync_wt_worker_env* we =
        (_amasync_wt_worker_env*) GC_MALLOC(sizeof(_amasync_wt_worker_env));
    we->_wt_inner_fn  = fn;
    we->_wt_inner_arg = (void*) (intptr_t) arg;
    we->_wt_done_ch   = done;
    AmalgameClosure* worker_closure =
        AmalgameClosure_new((void*) _amasync_wt_worker_fn, we);
    AmalgameFiber* worker = Amalgame_Async_FiberSpawn(worker_closure, 0);

    _amasync_wt_timer_env* te =
        (_amasync_wt_timer_env*) GC_MALLOC(sizeof(_amasync_wt_timer_env));
    te->_wt_worker  = worker;
    te->_wt_ms      = ms;
    te->_wt_done_ch = done;
    AmalgameClosure* timer_closure =
        AmalgameClosure_new((void*) _amasync_wt_timer_fn, te);
    AmalgameFiber* timer = Amalgame_Async_FiberSpawn(timer_closure, 0);

    i64 winner = Amalgame_Async_ChannelReceive(done);
    /* Belt-and-braces: cancel the loser (idempotent if already done). */
    Amalgame_Async_FiberCancel(worker);
    Amalgame_Async_FiberCancel(timer);
    return winner == 1 ? 1 : 0;
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
