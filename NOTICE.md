# NOTICE — amalgame-async

## Authorship

Copyright 2026 Bastien Mouget. Original work — see
`runtime/Amalgame_Async.h`.

Part of the Amalgame ecosystem
([github.com/amalgame-lang/Amalgame](https://github.com/amalgame-lang/Amalgame)).
External contributions are paused at the ecosystem level; see the
main repo's `CONTRIBUTING.md` for the policy.

AI tools (Anthropic Claude) were used during development. Per
the project's authorship policy, AI is treated as a tool, not a
co-author at law.

## Licence

Apache License 2.0. See `LICENSE` for the full text.

## Third-party content

**None vendored.** This package binds to two system libraries
that ship with every modern Unix-like target:

### ucontext (POSIX user-context switching)

`getcontext` / `makecontext` / `swapcontext` / `setcontext`
are part of POSIX libc. Linux glibc, macOS libSystem, FreeBSD
/ OpenBSD / NetBSD libc all ship them. macOS deprecated the
API for new code in 10.6 but ABI-stable since — every modern
coroutine library on macOS still uses it. Windows MSYS2 /
MinGW does **not** ship ucontext; the Windows backend
(Fibers API) is planned for v0.3.

### epoll (Linux I/O multiplexing)

`epoll_create1` / `epoll_ctl` / `epoll_wait` are Linux-kernel
syscalls available since 2.6.27 (2008). Used for the v0.2 I/O
backend to park fibers on socket / pipe readiness. The
equivalent BSD/macOS `kqueue` backend is planned for v0.2.1,
Windows `IOCP` for v0.3.

### libgc (Boehm garbage collector)

`GC_MALLOC` for fiber stacks, `GC_set_stackbottom` /
`GC_get_my_stackbottom` for stack-aware collection during
fiber execution. These are stable since bdwgc 7.6 (2015) and
ship with every distro's `libgc-dev` package. Already a
transitive dependency of every Amalgame package via the main
runtime.

libgc is distributed under a permissive licence very similar
to [X11](https://github.com/ivmai/bdwgc/blob/master/README.QUICK) —
public domain modifications with no copyleft virality,
Apache-2.0 compatible.

This package does not include or redistribute any ucontext or
libgc code; users obtain them through their OS package manager
(`libgc-dev` on Debian, etc.) — already a prereq of the main
Amalgame runtime, so no extra install step is needed.

## Trademarks

None claimed. "POSIX" is a registered trademark of the IEEE.
