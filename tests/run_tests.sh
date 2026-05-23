#!/bin/bash
# ─────────────────────────────────────────────────────
#  amalgame-async — Test Runner
#  Usage: ./tests/run_tests.sh [/path/to/amc]
#
#  Self-contained: no external service. ucontext is part of
#  POSIX libc on every Unix-like target (Linux, macOS, *BSD).
#  libgc-dev must expose GC_set_stackbottom — every distro
#  build does since Boehm GC 7.6.
# ─────────────────────────────────────────────────────

set -u

if [ $# -ge 1 ]; then
    AMC="$1"
elif [ -n "${AMC:-}" ]; then
    :
elif command -v amc >/dev/null 2>&1; then
    AMC="$(command -v amc)"
else
    echo "ERROR: amc not found." >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_RUNTIME="$PKG_ROOT/runtime"

AMC_DIR="$(cd "$(dirname "$AMC")" && pwd)"
if [ -d "$AMC_DIR/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/runtime"
elif [ -d "$AMC_DIR/../share/amalgame/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/../share/amalgame/runtime"
elif [ -n "${AMC_RUNTIME:-}" ]; then
    :
else
    echo "ERROR: amc runtime/ not found. Set AMC_RUNTIME=..." >&2
    exit 2
fi

BUILD_DIR="$(mktemp -d -t async-XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
PROJ_DIR="$BUILD_DIR/proj"
mkdir -p "$PROJ_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'
PASS=0; FAIL=0; SKIP=0

echo ""
echo "════════════════════════════════════════════"
echo "  amalgame-async — Tests"
echo "════════════════════════════════════════════"
echo "  amc:     $AMC ($("$AMC" --version 2>&1 | head -1))"
echo "  runtime: $AMC_RUNTIME"
echo ""

# ── Gate: libgc with GC_set_stackbottom ─────────────
echo "── Probing libgc stack-bottom switching ─────"
LIBGC_OK=0
cat > "$BUILD_DIR/_gcprobe.c" <<'EOF'
#include <gc.h>
int main(void) {
    struct GC_stack_base sb;
    GC_get_my_stackbottom(&sb);
    GC_set_stackbottom((void*)0, &sb);
    return 0;
}
EOF
if gcc "$BUILD_DIR/_gcprobe.c" -lgc -o "$BUILD_DIR/_gcprobe" 2>/dev/null; then
    LIBGC_OK=1
    echo "  GC_set_stackbottom:  found"
else
    echo "  GC_set_stackbottom:  NOT FOUND (libgc too old? 7.6+ required)"
fi
echo ""

# ── Stage fake cache for the test fixture ───────────
FAKE_CACHE="$BUILD_DIR/cache"
PKG_GIT="github.com/amalgame-lang/amalgame-async"
PKG_TAG="${PKG_TAG:-v0.1.0}"
FAKE_SHA="deadbeefcafebabe0000000000000000000000ab"
SHORT_SHA="${FAKE_SHA:0:8}"
PKG_CACHE_DIR="$FAKE_CACHE/$PKG_GIT/${PKG_TAG}_${SHORT_SHA}"

mkdir -p "$(dirname "$PKG_CACHE_DIR")"
rm -rf "$PKG_CACHE_DIR"
ln -s "$PKG_ROOT" "$PKG_CACHE_DIR"

cat > "$PROJ_DIR/amalgame.lock" <<EOF
[[package]]
name = "amalgame-async"
git  = "$PKG_GIT"
tag  = "$PKG_TAG"
rev  = "$FAKE_SHA"
EOF

export AMALGAME_PACKAGES_DIR="$FAKE_CACHE"

run_test() {
    local name="$1"
    local expected="$2"
    printf "  %-46s" "$name"
    if [ "$LIBGC_OK" = "0" ]; then
        echo -e "${YELLOW}SKIP${NC} (libgc missing GC_set_stackbottom)"
        SKIP=$((SKIP + 1)); return
    fi
    cp "$SCRIPT_DIR/stdlib_async.am" "$PROJ_DIR/test.am"
    local out_base="$PROJ_DIR/test"
    local out
    out=$(cd "$PROJ_DIR" && "$AMC" -o test test.am 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${RED}FAIL${NC} (amc)"; echo "$out" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    if [ ! -f "$out_base.c" ]; then
        echo -e "${RED}FAIL${NC} (no .c)"; FAIL=$((FAIL + 1)); return
    fi
    gcc -O2 -w \
        -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" \
        "$out_base.c" \
        -lgc -lm -lcurl -ldl -lpthread \
        -o "$out_base" 2>"$BUILD_DIR/link.log"
    if [ ! -x "$out_base" ]; then
        echo -e "${RED}FAIL${NC} (gcc link)"
        cat "$BUILD_DIR/link.log" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    local run_output
    run_output=$("$out_base" 2>&1)
    if echo "$run_output" | grep -qF "$expected"; then
        echo -e "${GREEN}PASS${NC}"; PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "    expected: $expected"
        echo "    got:      $(echo "$run_output" | head -5 | tr '\n' '|')"
        FAIL=$((FAIL + 1))
    fi
}

echo "── Async ───────────────────────────────────"
run_test "3 fibers run to completion"            "[PASS] 3 fibers run to completion"
run_test "round-robin yield ordering"            "[PASS] round-robin yield ordering"
run_test "sleep wake-time ordering"              "[PASS] sleep wake-time ordering"
run_test "channel parking producer-consumer"     "[PASS] channel parking producer-consumer"
run_test "RunUntil deadline + Pending count"     "[PASS] RunUntil deadline + Pending count"
run_test "FiberCurrentId distinct + 0 outside"   "[PASS] FiberCurrentId distinct + 0 outside"
run_test "channel drain-then-sentinel"           "[PASS] channel drain-then-sentinel after Close"

echo "── Async I/O (v0.2 — Linux epoll) ──────────"
run_test "MakeNonBlocking sets O_NONBLOCK"       "[PASS] MakeNonBlocking sets O_NONBLOCK"
run_test "WaitFdReadable wakes on byte arrival"  "[PASS] WaitFdReadable wakes on byte arrival"
run_test "WaitFdReadable timeout returns false"  "[PASS] WaitFdReadable timeout returns false"

echo "── Cancellation (v0.2.2) ──────────────────"
run_test "FiberCancel wakes a sleeping fiber"    "[PASS] FiberCancel wakes a sleeping fiber"
run_test "FiberCancel wakes channel-parked fbr"  "[PASS] FiberCancel wakes a channel-parked fiber"
run_test "pre-cancelled fiber early-outs"        "[PASS] pre-cancelled fiber early-outs at every yield"

# v0.2.1 regression: scheduler globals must be SHARED across translation
# units. Pre-v0.2.1 used `static` linkage on `_amasync_sched`, giving
# every .o file its own copy — a fiber spawned in one TU was invisible
# to FiberCurrentId() in another. Surfaced in production when
# amalgame-web's WebApp.Handle (web facade.o) dispatched the user route
# handler closure (lowered into the user app's demo.o): handler
# observed FiberCurrentId() == 0 even though it ran from inside the
# per-conn fiber that net-http's nethttp.o had spawned. Fixed by
# `__attribute__((weak))` so the linker merges every TU's copy into one.
echo "── Cross-TU scheduler sharing (regression) ──"
printf "  %-46s" "fiber spawned in lib seen from app"
if [ "$LIBGC_OK" = "0" ]; then
    echo -e "${YELLOW}SKIP${NC} (libgc missing GC_set_stackbottom)"
    SKIP=$((SKIP + 1))
else
    cat > "$BUILD_DIR/_xtu_lib.c" <<'CEOF'
#include "Amalgame_Async.h"
i64 xtu_lib_current_id(void) {
    return Amalgame_Async_FiberCurrentId();
}
CEOF
    cat > "$BUILD_DIR/_xtu_app.c" <<'CEOF'
#include "Amalgame_Async.h"
#include <stdio.h>
extern i64 xtu_lib_current_id(void);
static void* fiber_fn(void* env, void* arg) {
    (void) env; (void) arg;
    i64 app_id = Amalgame_Async_FiberCurrentId();
    i64 lib_id = xtu_lib_current_id();
    printf("app_id=%lld lib_id=%lld\n", (long long) app_id, (long long) lib_id);
    return NULL;
}
int main(void) {
    GC_INIT();
    AmalgameClosure* c = AmalgameClosure_new((void*) fiber_fn, NULL);
    Amalgame_Async_FiberSpawn(c, 0);
    Amalgame_Async_SchedulerRun();
    return 0;
}
CEOF
    gcc -O2 -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" -c "$BUILD_DIR/_xtu_lib.c" -o "$BUILD_DIR/_xtu_lib.o" 2>"$BUILD_DIR/_xtu.log"
    gcc -O2 -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" -c "$BUILD_DIR/_xtu_app.c" -o "$BUILD_DIR/_xtu_app.o" 2>>"$BUILD_DIR/_xtu.log"
    gcc -O2 "$BUILD_DIR/_xtu_app.o" "$BUILD_DIR/_xtu_lib.o" -lgc -lm -lcurl -ldl -lpthread \
        -o "$BUILD_DIR/_xtu" 2>>"$BUILD_DIR/_xtu.log"
    if [ ! -x "$BUILD_DIR/_xtu" ]; then
        echo -e "${RED}FAIL${NC} (build)"
        head -5 "$BUILD_DIR/_xtu.log" | sed 's/^/    /'
        FAIL=$((FAIL + 1))
    else
        XTU_OUT=$("$BUILD_DIR/_xtu")
        # Expect app_id and lib_id equal AND positive. Bug behavior: lib_id==0.
        AID=$(echo "$XTU_OUT" | sed -nE 's/.*app_id=([0-9]+).*/\1/p')
        LID=$(echo "$XTU_OUT" | sed -nE 's/.*lib_id=([0-9]+).*/\1/p')
        if [ -n "$AID" ] && [ "$AID" = "$LID" ] && [ "$AID" -gt 0 ] 2>/dev/null; then
            echo -e "${GREEN}PASS${NC} ($XTU_OUT)"
            PASS=$((PASS + 1))
        else
            echo -e "${RED}FAIL${NC} ($XTU_OUT — expected app_id == lib_id > 0)"
            FAIL=$((FAIL + 1))
        fi
    fi
fi

echo ""
echo "────────────────────────────────────────────"
echo -e "  ${GREEN}PASS: $PASS${NC}  |  ${RED}FAIL: $FAIL${NC}  |  ${YELLOW}SKIP: $SKIP${NC}"
echo "────────────────────────────────────────────"
