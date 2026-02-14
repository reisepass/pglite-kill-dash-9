# PGlite Crash Safety: Failure Modes & Fixes

This document describes the crash safety improvements to PGlite's NodeFS backend.
Each section covers a real failure mode, the developer behavior that triggers it,
and how the fix prevents it.

---

## Overview

Two fixes were added to `src/fs/nodefs.ts`:

1. **PID-based lock file** — prevents two PGlite instances from opening the same
   data directory simultaneously
2. **Partial initdb detection** — detects and wipes incomplete data directories
   left behind by kill -9 during first-time initialization

Both run only at startup. Zero overhead during normal database operation.

---

## Fix 1: PID-Based Lock File

### The Code

```
nodefs.ts: #acquireLock() / #releaseLock()
```

On `init()`, NodeFS creates a sibling lock file (`/path/to/mydb.lock` for
`/path/to/mydb`) containing the current process PID and timestamp. On `closeFs()`,
the lock is released.

The lock file is placed OUTSIDE the data directory because PostgreSQL rejects
unknown files inside its data directory (causes ExitStatus exit(1) in WASM).

### Lock Lifecycle

```
  Process A opens PGlite("./mydb")
  ┌─────────────────────────────────────────────┐
  │ 1. Check ./mydb.lock                        │
  │    - doesn't exist → proceed                │
  │    - exists, PID dead → stale, proceed      │
  │    - exists, PID alive → throw Error        │
  │                                              │
  │ 2. Write PID + timestamp to ./mydb.lock     │
  │                                              │
  │ 3. Mount filesystem, run PostgreSQL          │
  │    ... normal operation, no lock overhead... │
  │                                              │
  │ 4. closeFs() → delete ./mydb.lock           │
  └─────────────────────────────────────────────┘

  If Process A is kill -9'd:
  ┌─────────────────────────────────────────────┐
  │ ./mydb.lock still exists on disk            │
  │ but Process A's PID is now dead             │
  │                                              │
  │ Process B opens PGlite("./mydb")            │
  │ 1. Check ./mydb.lock → exists               │
  │ 2. Read PID → check process.kill(pid, 0)   │
  │ 3. PID is dead → stale lock, take over     │
  │ 4. Overwrite with own PID → proceed         │
  └─────────────────────────────────────────────┘
```

### Performance Impact

Lock acquisition adds these calls at startup ONLY:
- `fs.existsSync()` — one stat call
- `fs.readFileSync()` — only if lock file exists (read ~20 bytes)
- `fs.openSync()` + `fs.writeSync()` — write PID

Lock release at shutdown:
- `fs.closeSync()` + `fs.unlinkSync()`

During normal operation (queries, reads, writes): **zero overhead**.

---

### Failure Mode A: HMR / Dev Server Restart (2 tests)

**Tests:** `hmr-double-instance.test.js`

**What the developer does:**
```
1. Developer runs "npm run dev" (starts Vite/webpack dev server)
2. App opens PGlite("./mydb"), starts inserting data
3. Developer saves a file → HMR triggers
4. Dev server creates a NEW PGlite("./mydb") for the hot-reloaded module
5. The OLD instance is still alive (not yet garbage collected)
6. Two PGlite instances now access the same data directory
```

**What happens WITHOUT the fix:**
```
  Time ──────────────────────────────────────────────►

  Instance A (old module):
  ├── INSERT row 1 ──── INSERT row 3 ──── INSERT row 5 ──── ...
  │
  Instance B (new module, same ./mydb):
  ├────── INSERT row 2 ──── INSERT row 4 ──── INSERT row 6 ── ...
  │
  PostgreSQL WAL:
  ├── Both instances write WAL entries to the SAME files
  │   WAL segment gets interleaved writes from two backends
  │   Neither backend knows about the other's changes
  │
  Result: DATA CORRUPTION
  ├── Duplicate sequence values (both start from same serial)
  ├── Interleaved WAL entries cause recovery failures
  └── Index corruption from concurrent page modifications
```

**What happens WITH the fix:**
```
  Instance A opens ./mydb → acquires ./mydb.lock (PID 1234)
  Instance B opens ./mydb → reads ./mydb.lock → PID 1234 alive
                          → throws: "Data directory is locked by
                            another PGlite instance (PID 1234)"

  Instance B never touches the data directory.
  Instance A continues operating safely.
```

---

### Failure Mode B: Multiple Terminals / Scripts (5 tests)

**Tests:** `overlapping-instances.test.js` (5 scenarios)

**What the developer does:**
```
# Terminal 1
$ node server.js          # opens PGlite("./mydb")

# Terminal 2 (forgot Terminal 1 is running)
$ node seed-data.js       # also opens PGlite("./mydb")
```

Or with test runners that parallelize:
```
# Jest/Vitest running tests in parallel
# Test A and Test B both use PGlite("./test-db")
```

**The five scenarios tested:**

| Scenario | What Happens | Real-World Equivalent |
|----------|-------------|----------------------|
| Sequential overlap | A opens, B opens while A still running | Two scripts on same DB |
| Parallel start | A and B start simultaneously | Parallel test runners |
| Rapid-fire | A opens, kill, B opens, kill, C opens... | Rapid Ctrl+C and restart |
| Delayed overlap | A runs for a while, then B starts | Forgot about running server |
| Staggered writes | A and B alternate writing | Two cron jobs hitting same DB |

**Without the fix:** Both instances write to the same WAL files simultaneously,
causing interleaved WAL records that corrupt recovery.

**With the fix:** The second instance gets a clear error message telling the
developer which PID holds the lock, so they know exactly what to close.

---

## Fix 2: Partial initdb Detection

### The Code

```
nodefs.ts: #cleanPartialInit()
```

On `init()`, after acquiring the lock, NodeFS checks if the data directory
contains a partially-initialized database. PostgreSQL's initdb creates files
in stages over ~400ms:

```
  initdb timeline (measured on PGlite WASM)
  ─────────────────────────────────────────────────────────►

  ~0ms     PG_VERSION created
           Directory structure created
           postgresql.conf written
           postgresql.auto.conf written

  ~100ms   global/ directory populated
           base/1/ (template1) starts being built

  ~150ms   global/pg_control written

  ~200ms   System catalog tables created in base/1/
           (but NOT system views like pg_tables yet)

  ~350ms   System views (pg_tables, pg_views, ...) created
           template0 database created (base/4/)

  ~400ms   postgres database created (base/5/)
           initdb complete ✓

           ◄─── kill -9 at ANY point before this = corrupt ──►
```

The detection logic:

```
  #cleanPartialInit()
  │
  ├── Read data directory entries
  │   └── Empty? → return (nothing to clean)
  │
  ├── Check PG_VERSION exists
  │   └── No? → wipe all (very early interruption)
  │
  ├── Count subdirectories in base/
  │   └── < 3? → wipe all (mid-initdb interruption)
  │
  └── ≥ 3? → database is fully initialized, proceed normally
```

### Performance Impact

Runs at startup ONLY:
- `fs.readdirSync()` of data directory — one readdir
- `fs.existsSync()` for PG_VERSION — one stat
- `fs.readdirSync()` of base/ — one readdir (3 entries typically)

During normal operation: **zero overhead**.

On a fully initialized database, this adds ~0.1ms to startup.

---

### Failure Mode C: Kill During First-Time Initialization (1 test)

**Test:** `rapid-kill-cycles.test.js` → "should detect corruption from kill during first-time init"

**What the developer does:**
```
1. Developer runs their app for the first time
   (PGlite creates a new database — initdb runs)
2. Developer hits Ctrl+C before the app fully starts
   (or the process is OOM-killed during startup)
3. Developer runs the app again
```

**What happens WITHOUT the fix:**
```
  First run:
  ┌────────────────────────────────────────────────┐
  │ PGlite("./mydb")                               │
  │ initdb starts...                                │
  │   creates PG_VERSION                    (~0ms)  │
  │   creates postgresql.conf               (~0ms)  │
  │   creates global/pg_control           (~150ms)  │
  │   building template1 catalogs...      (~200ms)  │
  │                                                  │
  │   ██ KILL -9 at 250ms ██                        │
  │                                                  │
  │ Data directory now contains:                     │
  │   PG_VERSION ✓  pg_control ✓                    │
  │   base/1/ (partially built template1)           │
  │   NO template0 (base/4/)                        │
  │   NO postgres database (base/5/)                │
  │   NO pg_tables view                              │
  └────────────────────────────────────────────────┘

  Second run:
  ┌────────────────────────────────────────────────┐
  │ PGlite("./mydb")                               │
  │ Sees PG_VERSION → "database exists, resuming"  │
  │ _pgl_initdb() skips initialization             │
  │ _pgl_backend() starts PostgreSQL               │
  │                                                  │
  │ App: SELECT * FROM pg_tables                    │
  │ → ERROR: relation "pg_tables" does not exist    │
  │                                                  │
  │ ██ Aborted(). Build with -sASSERTIONS ██       │
  │    (Emscripten WASM fatal error)                │
  │                                                  │
  │ DATABASE IS PERMANENTLY BROKEN                   │
  │ Every future startup hits the same error.        │
  │ Only fix: manually delete ./mydb/ directory.     │
  └────────────────────────────────────────────────┘
```

**What happens WITH the fix:**
```
  First run: same as above — killed at 250ms

  Second run:
  ┌────────────────────────────────────────────────┐
  │ PGlite("./mydb")                               │
  │ #cleanPartialInit() runs:                       │
  │   PG_VERSION exists? YES                        │
  │   base/ subdirectories? 1 (only base/1/)       │
  │   1 < 3 → PARTIAL INIT DETECTED                │
  │   → wipe data directory                         │
  │                                                  │
  │ _pgl_initdb() runs fresh → creates new DB      │
  │ App starts normally ✓                           │
  └────────────────────────────────────────────────┘
```

---

### Failure Mode D: Burst-Mode Rapid Restarts (1 test)

**Test:** `wal-bloat-no-checkpoint.test.js` → "should survive burst-mode: 15 extremely rapid kill cycles"

**What the developer does:**
```
Developer has a script that watches for file changes and restarts:
  $ nodemon server.js

  1. Save file → nodemon kills server (SIGKILL) → restarts
  2. Syntax error → crash → developer fixes → save again
  3. This cycle repeats rapidly (every 300ms-1s)
  4. On a fresh project, the database hasn't finished initializing
     before the next kill comes
```

This is an extreme version of Failure Mode C: instead of one kill during
initdb, it's 15 rapid kills in succession. Each time:

```
  Cycle 0:  initdb starts → killed at 300ms → partial base/
  Cycle 1:  cleanPartialInit wipes → initdb starts → killed at 300ms
  Cycle 2:  cleanPartialInit wipes → initdb starts → killed at 300ms
  ...
  Cycle N:  cleanPartialInit wipes → initdb starts → finally completes!
            App works normally ✓
```

Without the fix, cycle 0 leaves a partial database, and every subsequent
cycle tries to use it and crashes with `Aborted()`. The database is stuck
in a permanently broken state that the developer can only fix by manually
deleting the data directory.

---

## Why Browser Backends Are Not Affected

These fixes are NodeFS-specific. Browser backends (IdbFS, OPFS-AHP) are
not vulnerable to these failure modes:

| Failure Mode | NodeFS | IdbFS | OPFS-AHP |
|-------------|--------|-------|----------|
| Overlapping instances | **Vulnerable** — two processes can open same files | Safe — each tab loads into memory, IndexedDB writes are atomic | Safe — `createSyncAccessHandle` is exclusive |
| Partial initdb | **Vulnerable** — kill -9 leaves torn files on disk | Safe — if tab dies, in-memory changes are lost but IndexedDB retains last consistent state | Mostly safe — browser doesn't have kill -9 equivalent |
| WAL corruption from kill | Possible but PostgreSQL recovery handles it (fsync works via NODEFS) | N/A — no direct file writes | Unlikely — browser ensures I/O completion on tab close |

The `// TODO` comment in nodefs.ts notes that Web Locks API would be a
good addition for browser backends to prevent data loss from concurrent
tab access, but it's a data consistency concern, not a crash corruption concern.

---

## Test Coverage Summary

Tests that were **failing before** and **pass after** our fixes:

| Test File | Test Name | Fixed By | Failure Mode |
|-----------|-----------|----------|-------------|
| `hmr-double-instance` | HMR replaces module, old instance still alive | Lock | A |
| `hmr-double-instance` | Rapid HMR replacement cycles | Lock | A |
| `overlapping-instances` | Sequential overlap | Lock | B |
| `overlapping-instances` | Parallel start | Lock | B |
| `overlapping-instances` | Rapid-fire open/kill cycles | Lock | B |
| `overlapping-instances` | Delayed overlap | Lock | B |
| `overlapping-instances` | Staggered writes | Lock | B |
| `rapid-kill-cycles` | Kill during first-time init | Partial init detection | C |
| `wal-bloat-no-checkpoint` | Burst-mode: 15 rapid 300ms kills | Partial init detection | D |
