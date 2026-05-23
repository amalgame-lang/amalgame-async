# amalgame-async

Cooperative concurrency for [Amalgame](https://github.com/amalgame-lang/Amalgame).
**Fiber**, **Channel**, **Scheduler** + **I/O** (epoll on Linux) —
stackful coroutines on POSIX `ucontext`, single-threaded
round-robin scheduler. Socket / pipe reads park the fiber instead
of blocking the OS thread. Pairs with
[`amalgame-threading`](https://github.com/amalgame-lang/amalgame-threading)
for CPU parallelism (one scheduler per OS thread; M:N is a v0.4
concern).

## Why this package, not `amalgame-threading`?

**Different mental models.** Threading is preemptive OS threads
with locks for shared state. Async is a cooperative scheduler
sharing one OS thread, with yields for scheduling and channels
for state transfer. Mixing them would force every `Mutex`
consumer to embed a scheduler and confuse `Fiber` vs `Thread`.

**They compose as orthogonal layers.** When you need CPU
parallelism, spawn N OS threads via `amalgame-threading` and
run a scheduler in each — that's M:N scheduling, planned for
v0.4. Today async is single-threaded by design (no locks
needed, fewer footguns).

## Prerequisites

`ucontext` is part of POSIX libc on every supported Unix-like
target, and `libgc-dev` (with `GC_set_stackbottom`, shipped
since Boehm GC 7.6 / 2015) is **already a dependency of the
main Amalgame runtime**. So **no extra install command** if
you've already got `amc` working.

| OS / distro | What you need (already there if amc runs) |
|---|---|
| Debian / Ubuntu | `libgc-dev` |
| Fedora / RHEL | `gc-devel` |
| Arch / Manjaro | `gc` |
| Alpine | `gc-dev` |
| macOS | libSystem (built-in) + Homebrew `bdw-gc` |
| Windows (MSYS2 / MinGW) | **not supported in v0.1** — Fibers API backend planned for v0.2 |

## Install

```bash
amc package add async                                # via index
amc package add github.com/amalgame-lang/amalgame-async@v0.2.0
```

Requires **amc 0.8.19+**.

## Surface (v0.2)

```amalgame
import Amalgame.Async

public class Program {
    public static void Main(string[] args) {

        // ── Bounded FIFO between fibers ───────────────
        let ch = Async.ChannelNew(8)

        // ── Producer fiber: 5 values, then close ──────
        let producer = (_x: int) => {
            var i: int = 0
            while (i < 5) {
                Async.ChannelSend(ch, 100 + i)
                i = i + 1
            }
            Async.ChannelClose(ch)
            return 0
        }

        // ── Consumer fiber: pull until drained ────────
        let consumer = (_x: int) => {
            while (true) {
                let v: int = Async.ChannelReceive(ch)
                if (v == 0 && Async.ChannelIsClosed(ch)) { break }
                Console.WriteLine("got " + String_FromInt(v))
            }
            return 0
        }

        Async.FiberSpawn(producer, 0)
        Async.FiberSpawn(consumer, 0)
        Async.SchedulerRun()
    }
}
```

### v0.2.0 method surface

| Method | Returns | Notes |
|---|---|---|
| **Fiber** | | |
| `Async.FiberSpawn(closure, arg)` | `AmalgameFiber*` | Queues a 1-arg closure; runs when `SchedulerRun` pumps it |
| `Async.FiberYield()` | `void` | Cooperative yield — moves caller to ready queue tail |
| `Async.FiberSleep(ms)` | `void` | Parks caller; scheduler advances other fibers meanwhile. Outside a fiber, falls back to `nanosleep` |
| `Async.FiberCurrentId()` | `int` | Integer id of running fiber; 0 outside any fiber |
| **Channel** — bounded FIFO of i64-erased values | | |
| `Async.ChannelNew(capacity)` | `AmalgameAsyncChannel*` | capacity ≥ 1 |
| `Async.ChannelSend(ch, value)` | `bool` | **Parks fiber if full**; false if closed |
| `Async.ChannelReceive(ch)` | `int` | **Parks fiber if empty**; **0 sentinel** if closed + empty |
| `Async.ChannelTrySend(ch, value)` | `bool` | Non-blocking; false if full or closed |
| `Async.ChannelTryReceive(ch)` | `int` | Non-blocking; 0 if empty |
| `Async.ChannelClose(ch)` | `void` | Wakes every parked fiber on the channel |
| `Async.ChannelIsClosed(ch)` | `bool` | |
| `Async.ChannelCount(ch)` | `int` | Current buffered count |
| `Async.ChannelCapacity(ch)` | `int` | Capacity passed at New time |
| **Scheduler** | | |
| `Async.SchedulerRun()` | `void` | Pump until ready + sleeping + waiting + fd-wait queues all empty |
| `Async.SchedulerRunUntil(ms)` | `void` | Same, but stop after `ms` milliseconds |
| `Async.SchedulerPending()` | `int` | Count of fibers still alive (ready + sleeping + waiting) |
| **I/O — Linux only (v0.2.0)** | | |
| `Async.WaitFdReadable(fd, timeout_ms)` | `bool` | Parks fiber until `fd` is readable. Negative timeout = wait forever. Returns `true` on ready, `false` on timeout/error. **Must be called from inside a fiber.** |
| `Async.WaitFdWritable(fd, timeout_ms)` | `bool` | Same for writability |
| `Async.MakeNonBlocking(fd)` | `bool` | Sets `O_NONBLOCK` via `fcntl` — mandatory companion to `WaitFd*` for the read/write loop pattern |

### Value erasure

Channels store values as `void*` at the C level (same as
`AmalgameList` and `amalgame-threading` channels). For
primitive payloads (`int`, `bool`, enums) the manifest declares
`Send`/`Receive` as `i64` so amc handles the boxing/unboxing
automatically. For pointer payloads (`List<X>*`, your own
class), they round-trip cleanly.

The **0 sentinel** for closed-and-empty `Receive` matches
`amalgame-threading`'s convention — pair `Receive` with
`IsClosed()` when `0` is a legitimate value.

### Closure model

`FiberSpawn` takes a 1-arg `AmalgameClosure` plus a single
`int` arg passed through. If you don't need the arg, write
`let work = (_x: int) => { ... }` and pass `0`. The closure
captures enclosing locals by value at creation time — the
fiber sees a **snapshot**. Mutate shared state through
`Channel` (no `Mutex` needed since the scheduler is
single-threaded; reads/writes between yields are atomic by
construction).

## bdwgc safety

Each fiber owns a `GC_MALLOC`'d stack block — libgc scans it
as a regular GC object, so locals on a parked fiber's stack
stay reachable. While a fiber is running, the scheduler
updates the collector's stack bottom via `GC_set_stackbottom`
so a collection triggered mid-fiber scans the right region.
On yield, the main thread's original stack bottom is restored.

You don't need to think about any of this from AM code — just
use `Async.FiberSpawn` and let the scheduler handle the
plumbing.

## Composing with `amalgame-threading`

For CPU parallelism, spawn N OS threads via
`amalgame-threading` and run a scheduler in each:

```amalgame
import Amalgame.Threading
import Amalgame.Async

let worker = (_id: int) => {
    // Each thread gets its own scheduler with its own fibers
    Async.FiberSpawn(serverLoop, 0)
    Async.FiberSpawn(metricsLoop, 0)
    Async.SchedulerRun()
    return 0
}
let t1 = Threading.ThreadSpawn(worker, 1)
let t2 = Threading.ThreadSpawn(worker, 2)
let t3 = Threading.ThreadSpawn(worker, 3)
let t4 = Threading.ThreadSpawn(worker, 4)
```

⚠️ v0.1 caveat: the scheduler uses **process-wide globals**.
Running schedulers concurrently from multiple threads
**will corrupt the queues**. M:N (per-thread schedulers via
TLS) is planned for v0.4. Until then, treat
`Async.SchedulerRun()` as a single-thread call.

For shared state across threads (not fibers within one
thread), use `Threading.Channel` rather than
`Async.Channel` — `Threading.Channel` uses pthread mutexes
and is safe across OS threads; `Async.Channel` assumes one
thread + cooperative scheduling.

## I/O integration — read/write pattern

For an async-friendly socket read, set `O_NONBLOCK` once, then
loop: read until `EAGAIN`, then `WaitFdReadable`, then read again.

```amalgame
Async.MakeNonBlocking(connFd)
Async.FiberSpawn((fd: int) => {
    let ok: bool = Async.WaitFdReadable(fd, 30000)   // 30s
    if (!ok) { return 0 }                            // timeout
    // ... now read(fd, ...) won't block; if it returns -1 with
    // errno=EAGAIN, loop back to WaitFdReadable
    return 0
}, connFd)
```

The next-version coordinated PR in `amalgame-net-http` will ship
`Http1.ServeAsync(port, handler)` that does this dance internally
— for v0.2 user code drives the loop manually.

**Backend coverage:**

| Platform | I/O backend | Status |
|---|---|---|
| Linux | `epoll` | ✅ v0.2.0 |
| BSD + macOS | `kqueue` | 🟡 planned v0.2.1 |
| Windows | `IOCP` (after Fibers backend) | 🟡 planned v0.3 |

On unsupported platforms the package still builds and the
fiber/channel/scheduler surface works; only `WaitFd*` is
disabled (returns `false` with a compile-time `#warning`).

## v0.2.3 — `Async.WithTimeout` ergonomic helper

```amalgame
let done: bool = Async.WithTimeout(work, 0, 5000)   // 5s budget
if (done) {
    // work() completed
} else {
    // deadline fired — work's fiber was FiberCancel'd mid-flight
}
```

Wraps the spawn + timer + cancel + channel-race dance into a single
call. Two helper fibers race a 1-capacity channel: the worker runs
the user closure and `TrySend(1)`; the timer `FiberSleep(ms)` then
`TrySend(2)` + `FiberCancel(worker)`. The caller `ChannelReceive`s
and returns the winner. Both fibers get cancelled at the end
(idempotent if already done).

**Composable.** Nest `WithTimeout` inside another `WithTimeout` —
the outer's cancel propagates to the inner via `FiberCancel` and
the inner worker observes it at its next yield point.

## v0.2.2 — cooperative cancellation

`Async.FiberCancel(f)` flips a flag on the target fiber AND wakes it
if it's parked on a sleep, an `fd` wait, or a channel queue. The
fiber observes the wake exactly like a normal resume — `FiberSleep`
returns, `WaitFd*` returns `false`, `ChannelSend/Receive` return
`false` / 0. Use `Async.IsCancelled()` at the yield point to detect
the difference between a real event and a cancellation.

```amalgame
let worker: AmalgameFiber = Async.FiberSpawn(serveOneRequest, conn)
// Cancel the worker if it's still alive 30s from now.
Async.FiberSpawn((_x: int) => {
    Async.FiberSleep(30000)
    Async.FiberCancel(worker)
    return 0
}, 0)
```

Use cases:
- **Graceful shutdown.** Track per-conn fibers in a set; on SIGTERM,
  iterate and `FiberCancel` each one so in-flight handlers wake and
  return promptly.
- **Request timeouts.** Spawn the work + a timer fiber that cancels
  the work if it overstays.
- **Wait for first of N events.** Spawn N fiber waiters; cancel the
  losers once the first one returns.

Cooperative model — the cancelled fiber chooses when to return.
Tight `while (!Async.IsCancelled())` loops finish promptly; opaque
`AmalgameClosure_call1` to user code keeps running until it
voluntarily hits a yield point. No preemption, no async-signal-safe
juggling, no surprises.

## v0.2.1 — cross-TU scheduler sharing fix (critical)

Pre-v0.2.1 the scheduler global `_amasync_sched` was declared
`static` inside the header. When more than one `.o` file in the
final binary included `Amalgame_Async.h`, each `.o` got its own
private copy of the scheduler. A fiber spawned in one TU (e.g. the
HTTP server `nethttp.o`) was invisible to `FiberCurrentId()` called
from another TU (e.g. the user app `demo.o`) — so any user handler
dispatched through a framework layer (`amalgame-web`'s
`WebApp.Handle`) ran with `FiberCurrentId() == 0` and `FiberSleep`
silently fell back to `nanosleep`, blocking the OS thread and
serialising the entire server.

Fix: `__attribute__((weak)) AmalgameAsyncScheduler _amasync_sched`
— the linker merges every TU's copy into one. Verified via a new
regression test in `tests/run_tests.sh` that builds two object
files which both include the header and asserts they observe the
same `FiberCurrentId()`.

**Upgrade strongly recommended** if you use `amalgame-async` with
any package that depends on it transitively (notably
`amalgame-net-http` v0.9.1+ and `amalgame-web` v0.12.0+).

## Deferred to v0.3+

- **kqueue backend** (BSD + macOS) — v0.2.1
- **Per-fd multi-fiber wait lists** — today only one fiber can
  wait on a given fd at a time; a second `WaitFd*` on the same
  fd overwrites the first
- **Windows backend** (`ConvertThreadToFiber` / `SwitchToFiber`)
  for MinGW + IOCP I/O. Today's `ucontext` path doesn't exist
  on Windows
- **Timer wheel** for >1k concurrent sleepers (v0.3). Today's
  sorted-insertion sleep list is O(N) per `Sleep`
- **`Async.Select`** multi-channel + multi-fd readiness (v0.3)
- **M:N scheduling** — one scheduler per OS thread, work
  stealing, TLS for current scheduler (v0.4)
- **`async` / `await`** language sugar in amc — a separate
  language proposal, would desugar to `FiberSpawn` + WaitFd
  patterns shown here

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

10 tests, all self-contained — 7 covering fiber/channel/scheduler
basics, 3 covering I/O parking (`MakeNonBlocking`,
`WaitFdReadable` byte-arrival, `WaitFdReadable` timeout). The I/O
tests use an inline `@c {}` block to open a `pipe()` and exercise
the epoll integration end-to-end.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).
`ucontext` is libc-provided; `libgc` is X11-style permissive
(Apache-2.0 compatible).
