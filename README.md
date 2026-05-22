# amalgame-async

Cooperative concurrency for [Amalgame](https://github.com/amalgame-lang/Amalgame).
**Fiber**, **Channel**, **Scheduler** — stackful coroutines on
POSIX `ucontext`, single-threaded round-robin scheduler. Pairs
with [`amalgame-threading`](https://github.com/amalgame-lang/amalgame-threading)
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
amc package add github.com/amalgame-lang/amalgame-async@v0.1.0
```

Requires **amc 0.8.19+**.

## Surface (v0.1)

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

### v0.1.0 method surface

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
| `Async.SchedulerRun()` | `void` | Pump until ready + sleeping + waiting queues all empty |
| `Async.SchedulerRunUntil(ms)` | `void` | Same, but stop after `ms` milliseconds |
| `Async.SchedulerPending()` | `int` | Count of fibers still alive (ready + sleeping + waiting) |

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

## Deferred to v0.2+

- **Async I/O** (`epoll` Linux / `kqueue` BSD+macOS / `IOCP`
  Windows). `Async.ReadFd(fd)` / `Async.WriteFd(fd)` that
  park the fiber until the fd is ready. Coordinated PR in
  `amalgame-net-http` to register sockets with the scheduler.
- **Windows backend** (`ConvertThreadToFiber` / `SwitchToFiber`)
  for MinGW. Today's `ucontext` path doesn't exist on Windows.
- **Timer wheel** for >1k concurrent sleepers (v0.3). Today's
  sorted-insertion sleep list is O(N) per `Sleep`.
- **`Async.Select`** multi-channel readiness (v0.3).
- **M:N scheduling** — one scheduler per OS thread, work
  stealing, TLS for current scheduler (v0.4).
- **`async` / `await`** language sugar in amc — a separate
  language proposal, would desugar to `FiberSpawn` +
  `ChannelReceive` patterns shown here.

## Tests

```bash
./tests/run_tests.sh /path/to/amc
```

7 tests, all self-contained. Includes a multi-fiber
producer-consumer that exercises channel parking + waking
through a capacity-1 channel, and a sleep-ordering test that
verifies the sleep queue's wake-time priority.

## Licence

Apache-2.0 — see [`LICENSE`](LICENSE) and [`NOTICE.md`](NOTICE.md).
`ucontext` is libc-provided; `libgc` is X11-style permissive
(Apache-2.0 compatible).
