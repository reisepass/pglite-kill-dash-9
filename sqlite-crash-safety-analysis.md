# SQLite Crash Safety Analysis for PGlite

## Purpose

This report analyzes how SQLite achieves crash safety — specifically resilience to `kill -9`, power loss, and OS crashes — with the goal of identifying what PGlite needs to adopt to achieve similar robustness. The analysis is based on reading the SQLite/libSQL source code and official documentation.

---

## Table of Contents

1. [Why SQLite Survives `kill -9` and PGlite Doesn't](#1-why-sqlite-survives-kill--9-and-pglite-doesnt)
2. [SQLite's Crash Safety Architecture](#2-sqlites-crash-safety-architecture)
3. [The Atomic Commit Protocol (Rollback Journal)](#3-the-atomic-commit-protocol-rollback-journal)
4. [Write-Ahead Log (WAL) Mode](#4-write-ahead-log-wal-mode)
5. [fsync Strategy and Ordering Guarantees](#5-fsync-strategy-and-ordering-guarantees)
6. [File Locking and Concurrent Access Prevention](#6-file-locking-and-concurrent-access-prevention)
7. [Crash Recovery on Open](#7-crash-recovery-on-open)
8. [Power-Safe Overwrite (PSOW)](#8-power-safe-overwrite-psow)
9. [SQLite's Testing Methodology for Crash Safety](#9-sqlites-testing-methodology-for-crash-safety)
10. [libSQL Improvements Over SQLite](#10-libsql-improvements-over-sqlite)
11. [PGlite's Current State](#11-pglites-current-state)
12. [Gap Analysis: PGlite vs SQLite](#12-gap-analysis-pglite-vs-sqlite)
13. [Recommendations for PGlite](#13-recommendations-for-pglite)

---

## 1. Why SQLite Survives `kill -9` and PGlite Doesn't

The fundamental difference is **design philosophy**:

- **SQLite** assumes the process can die at **any point** between any two syscalls. Every write operation is structured so that no matter when a crash occurs, the database is recoverable on next open.
- **PGlite** (via PostgreSQL) assumes a **long-running managed server** with an orderly shutdown sequence. The `close()` method must run to checkpoint the WAL and finalize the filesystem. If it doesn't run (e.g., `kill -9`), the WAL is left inconsistent and Postgres panics on next startup.

`kill -9` sends `SIGKILL`, which cannot be caught, blocked, or ignored. No userland cleanup code runs. Any database that requires cleanup code to run for consistency is inherently unsafe against `SIGKILL`.

---

## 2. SQLite's Crash Safety Architecture

SQLite's crash safety rests on four pillars:

### Pillar 1: Write-Ahead Logging / Journaling
Every modification is logged to a separate file **before** touching the main database. The log is designed to be self-validating via checksums and salt values.

### Pillar 2: Ordered fsync Barriers
Critical `fsync()` calls enforce a strict ordering: the log must be durable on disk **before** the database is modified. This is not optional — it's the core invariant.

### Pillar 3: Self-Healing on Open
When SQLite opens a database, it checks for stale journals/WAL files and automatically recovers. No external tool or process is needed.

### Pillar 4: File Locking
POSIX advisory locks prevent multiple processes from writing simultaneously, eliminating an entire class of corruption bugs.

---

## 3. The Atomic Commit Protocol (Rollback Journal)

**Source code**: `pager.c` (7834 lines), `os_unix.c`

SQLite's rollback journal commit follows a precise 10-step sequence:

```
Step 1:  Acquire SHARED lock (allows reading)
Step 2:  Read pages into memory
Step 3:  Acquire RESERVED lock (signals write intent, readers still OK)
Step 4:  Create rollback journal with original page content
         - Journal header initially has page count = 0
Step 5:  Modify pages in memory only
Step 6:  *** FSYNC the journal to disk ***
         - This is the critical barrier
         - Journal page count is updated AFTER this fsync
Step 7:  Escalate to EXCLUSIVE lock (wait for all readers to finish)
Step 8:  Write modified pages to database file
Step 9:  *** FSYNC the database file ***
Step 10: Delete/truncate/zero the journal
         - THIS is the atomic commit point
```

### Why This Survives Crashes

**Crash before Step 6**: Journal is incomplete or has page count = 0. On recovery, SQLite sees zero pages in the journal → no rollback needed → database is untouched.

**Crash between Steps 6 and 10**: Journal is complete and fsynced. On recovery, SQLite detects a "hot journal" → replays original pages back to the database → transaction is rolled back.

**Crash after Step 10**: Journal is gone (or zeroed). Database has committed changes. Nothing to recover.

The key insight: **there is exactly one atomic operation that represents the commit** — deleting/zeroing the journal file. Everything before that point is uncommitted; everything after is committed. File deletion is atomic from the filesystem's perspective.

### The Zero-Page-Count Trick

The journal header's page count starts at zero and is only updated **after** the journal content is fsynced. This prevents a partially-written journal from being replayed:

```c
// pager.c - syncJournal()
// 1. Write all page records to journal
// 2. fsync the journal file
// 3. THEN update nRec (page count) in header
// 4. fsync again with SQLITE_SYNC_DATAONLY
```

If the process crashes between steps 1 and 3, the journal has `nRec = 0`, so recovery skips it entirely.

---

## 4. Write-Ahead Log (WAL) Mode

**Source code**: `wal.c` (4621 lines)

WAL mode inverts the journal approach: instead of writing original pages to a journal before modifying the database, WAL mode **never modifies the main database during normal operations**. All changes are appended to a WAL file.

### WAL File Structure

```
[WAL Header - 32 bytes]
  - Magic number: 0x377f0682 or 0x377f0683
  - Format version: 3007000
  - Page size
  - Checkpoint sequence number
  - Salt-1 and Salt-2 (random, change on WAL restart)
  - Header checksum

[Frame 1 - 24 bytes header + page_size data]
  - Page number
  - nTruncate (non-zero = this is a commit frame)
  - Salt-1, Salt-2 (must match header)
  - Checksum-1, Checksum-2

[Frame 2...]
[Frame N...]
```

### How WAL Commits Work

A transaction commits by appending a frame with `nTruncate > 0` (the database size after commit). The commit sequence:

```c
// wal.c - walFrames() - lines 4015-4242

1. If WAL was just restarted, write new WAL header with fresh salts
   → fsync header if syncHeader flag is set (line 4098-4100)

2. For each dirty page:
   → Encode frame header with cumulative checksums
   → Write frame header + page data to WAL file

3. If this is a commit (isCommit=1):
   → Pad to sector boundary if !POWERSAFE_OVERWRITE
   → *** FSYNC the WAL file *** (line 4189)

4. Update the WAL index (shared memory):
   → Append frame entries to wal-index hash table
   → Update mxFrame (highest valid frame number)
   → Write wal-index header using double-buffer technique
```

### Why WAL Survives Crashes

**Crash before the commit-frame fsync**: The WAL file contains frames but no valid commit record. On recovery, SQLite scans the WAL, validates each frame's checksum and salt, and stops at the last valid commit frame. Uncommitted frames are silently discarded.

**Crash after the commit-frame fsync**: The commit frame is durable. Recovery finds it and includes all frames up to that point.

### Frame Validation (The Core Safety Mechanism)

```c
// wal.c - walIndexRecover() - lines 1384-1608

For each frame in the WAL file:
  1. Read frame header (24 bytes)
  2. Read frame data (page_size bytes)
  3. Verify salt-1 and salt-2 match WAL header
  4. Compute running checksum and compare to frame's checksum
  5. If ANY check fails → STOP scanning, discard this and all subsequent frames
  6. If nTruncate > 0 → This is a commit boundary, update mxFrame
```

The checksum is **cumulative** — each frame's checksum includes all previous frames. This means a single corrupted byte anywhere in the WAL chain invalidates everything after it, preventing partial replays.

### Salt Values Prevent Generation Confusion

When a WAL checkpoint completes and the WAL is restarted, new random salt values are written to the header. Old frames (from the pre-checkpoint WAL) have the old salt values and are automatically rejected during recovery. This prevents stale frames from a previous generation from contaminating the current state.

### Double-Buffered WAL Index Header

```c
// wal.c - walIndexWriteHdr() - lines 942-954

1. Write header to aHdr[1] (secondary copy)
2. walShmBarrier()  // memory barrier
3. Write header to aHdr[0] (primary copy)
```

Readers always find a consistent header: if aHdr[0] and aHdr[1] disagree, a full recovery scan is triggered.

### WAL Checkpointing

Checkpointing transfers WAL frames back to the main database file:

```c
// wal.c - walCheckpoint() - lines 2193-2375

1. *** FSYNC the WAL file *** (line 2260)
2. Copy frames from WAL to database file, respecting reader positions
   → Only copy frames older than ALL active readers' marks
3. *** FSYNC the database file *** (line 2314)
4. Atomically update nBackfill counter
```

Checkpoints are triggered automatically when the WAL reaches ~1000 pages (~4MB). Three modes exist:
- **PASSIVE**: Best-effort, doesn't block readers
- **FULL**: Waits for all readers, completes fully
- **TRUNCATE**: Like FULL, but also truncates WAL file to zero

---

## 5. fsync Strategy and Ordering Guarantees

**Source code**: `os_unix.c` lines 3778-3954

### Where SQLite Calls fsync

| When | What is fsynced | Why |
|------|----------------|-----|
| After writing journal pages | Journal file | Ensure undo data is durable before modifying DB |
| After updating journal header | Journal file (DATAONLY) | Ensure page count is durable |
| After writing DB pages | Database file | Ensure changes are durable |
| After journal deletion | Directory | Ensure directory entry removal is durable |
| After WAL restart | WAL header | Prevent header from being lost on crash |
| After WAL commit frame | WAL file | Ensure commit record is durable |
| During WAL checkpoint | WAL, then DB | Ensure WAL data is safe before overwriting DB |

### Platform-Specific fsync

```c
// os_unix.c - full_fsync() - lines 3778-3849

if (macOS && HAVE_FULLFSYNC) {
    // F_FULLFSYNC forces disk write cache flush
    // Regular fsync() on macOS only flushes to disk controller, NOT to platters
    rc = fcntl(fd, F_FULLFSYNC, 0);
    if (rc) rc = fsync(fd);  // fallback
} else if (Apple) {
    // Always use fsync, never fdatasync (Apple HFS bug)
    rc = fsync(fd);
} else {
    // Linux/other: prefer fdatasync (skips metadata, faster)
    rc = fdatasync(fd);
    if (rc == ENOTSUP) rc = fsync(fd);  // fallback
}
```

**Critical macOS caveat**: On macOS, `fsync()` only guarantees data reaches the disk controller's write cache, NOT the physical platters. True durability requires `F_FULLFSYNC`. SQLite supports this via `PRAGMA fullfsync = ON`, but it's **off by default**. This means default SQLite on macOS is not fully crash-safe against power loss (though it IS safe against `kill -9`, since the OS cache is not lost).

### Directory fsync

```c
// os_unix.c - lines 3937-3953
// After creating/deleting journal files, fsync the directory entry

if (DIRSYNC flag is set) {
    open(directory_path, O_RDONLY);
    full_fsync(dirfd, 0, 0);
    close(dirfd);
}
```

This ensures that if power fails right after creating a journal file, the directory entry for the journal is durable. Without this, the journal file could exist on disk but be invisible to the filesystem after reboot.

---

## 6. File Locking and Concurrent Access Prevention

**Source code**: `os_unix.c` lines 1866-2200

### Lock Levels

```
UNLOCKED → SHARED → RESERVED → PENDING → EXCLUSIVE

SHARED:     Multiple readers allowed. No writing.
RESERVED:   One writer preparing. Readers still OK. No other writers.
PENDING:    Writer waiting. No NEW readers. Existing readers finish.
EXCLUSIVE:  Writer active. No other access at all.
```

### Implementation (POSIX Advisory Locks)

SQLite uses byte-range `fcntl()` locks on specific offsets in the database file:

```c
// Lock byte positions (os.h)
PENDING_BYTE   = 0x40000000          // 1 byte
RESERVED_BYTE  = PENDING_BYTE + 1    // 1 byte
SHARED_FIRST   = PENDING_BYTE + 2    // 510 bytes
SHARED_SIZE    = 510

// To acquire SHARED: read-lock on SHARED_FIRST..SHARED_FIRST+SHARED_SIZE
// To acquire RESERVED: write-lock on RESERVED_BYTE (1 byte)
// To acquire EXCLUSIVE: write-lock on entire SHARED range
```

### Why This Matters for Corruption Prevention

Without locking, two processes writing to the same database simultaneously will corrupt it. SQLite's lock escalation path ensures:

1. Only one writer at a time (RESERVED prevents a second writer)
2. Writers wait for readers to finish before modifying the file (PENDING → EXCLUSIVE)
3. Readers can detect that a writer is active and avoid reading inconsistent state

### PGlite Has No Locking

PGlite issue #85 (open since June 2024) tracks the lack of any locking mechanism. Opening the same data directory from two PGlite instances silently corrupts the database. This is the single most common source of corruption reports in PGlite's issue tracker.

---

## 7. Crash Recovery on Open

### Hot Journal Detection (Rollback Mode)

```c
// pager.c - hasHotJournal() - lines 5134-5225

A journal is "hot" if ALL of these are true:
  1. Journal file exists on disk
  2. No process holds a RESERVED or greater lock on the database
  3. Database file size > 0 bytes
  4. First byte of journal is non-zero (not zeroed/invalidated)
```

If a hot journal is detected on open:

```c
// pager.c - lines 5266-5388

1. Acquire EXCLUSIVE lock (skip RESERVED to prevent race)
2. fsync the hot journal (in case it was only in OS cache)
3. Call pager_playback() with isHot=1:
   → Read journal header
   → For each page record:
     → Read original page number and data from journal
     → Write original page back to database
     → Stop on short read (incomplete journal)
   → Truncate database to original size
   → fsync database
   → Delete/zero journal
```

### WAL Recovery on Open

```c
// wal.c - walIndexRecover() - lines 1384-1608

1. Read WAL file size
2. Read and validate WAL header:
   → Check magic number
   → Verify page size is power of 2
   → Validate header checksum
3. Scan all frames sequentially:
   → For each frame: validate salt + checksum
   → On first invalid frame: STOP (discard rest)
   → On valid commit frame (nTruncate > 0): update mxFrame
4. Rebuild wal-index from valid frames
5. Set nBackfill = 0 (force re-checkpoint)
6. Write recovered wal-index header
```

**Key property**: Recovery is a **single sequential pass** over the WAL file. It requires no random access, no external state, and no coordination with other processes. The WAL file is entirely self-describing.

---

## 8. Power-Safe Overwrite (PSOW)

**Source code**: `wal.c` lines 1732-1733, `pager.c` lines 4304-4367

PSOW means: when overwriting bytes X through Y in a file, no bytes outside that range will be affected, even during a crash mid-write. Modern disks (with 4096-byte sectors) generally provide this guarantee because on-board capacitors can complete in-progress sector writes during power loss.

### Impact on SQLite Behavior

**With PSOW** (default since SQLite 3.7.10):
- WAL frames don't need padding to sector boundaries
- Journal entries only need to save the specific pages being modified

**Without PSOW**:
- WAL commit frames are padded to sector boundaries to prevent a torn write from corrupting the commit record
- Journal entries must save all pages that share a sector with any modified page

```c
// wal.c - lines 4173-4186
if (!POWERSAFE_OVERWRITE) {
    // Pad the last frame to the next sector boundary
    // by repeating the final commit frame
    while (iOffset < iSectorBound) {
        write duplicate commit frame
        iOffset += szFrame
    }
}
```

**Important**: SQLite **never** assumes page writes are atomic, regardless of PSOW. A torn page (partially written) is always handled by the journal/WAL — PSOW only controls whether *adjacent* pages can be damaged.

---

## 9. SQLite's Testing Methodology for Crash Safety

SQLite has **590x more test code than production code** (92 MSLOC tests vs 156 KSLOC production). Crash safety specifically is tested via:

### VFS-Based Crash Simulation

A special VFS intercepts all I/O and simulates crashes:

1. **Spawn a child process** that runs SQLite operations
2. At a **randomly chosen I/O operation**, terminate the child
3. Before termination, **reorder and corrupt all unfsynced writes** — writes after the last fsync but before the crash are randomly reordered, partially applied, or filled with garbage
4. Parent process opens the database and verifies:
   - `PRAGMA integrity_check` passes
   - Transaction either fully committed or fully rolled back (no partial state)

### In-Memory Crash Simulation (TH3)

For embedded systems without `fork()`:

1. An in-memory VFS snapshots the filesystem at operation N
2. SQLite runs operations normally
3. Filesystem reverts to snapshot, with random damage applied to simulate power loss
4. Database is opened and verified for consistency
5. Loop increments N until operations complete without hitting a snapshot

### Journal-Test VFS

A specialized VFS that monitors ALL I/O between the database and journal, verifying that **nothing is ever written to the database that hasn't first been written and fsynced to the journal**. This directly validates the write-ordering invariant.

### Crash Test Files in Source

The SQLite source includes 17 dedicated crash test files:
- `crash.test`, `crash2.test` through `crash8.test`
- `walcrash.test`, `walcrash2.test`, `walcrash3.test`, `walcrash4.test`
- `crashM.test` (multi-process crash scenarios)

### C-Level Crash Test (`crashtest1.c`)

```c
// Forks child processes that perform random operations
// Randomly kills processes mid-transaction (simulating crash)
// Parent verifies database integrity after each crash
// Runs 10,000 iterations
```

### The SQLite Team's Position

> *"From this experience, the developers of SQLite feel confident that any other database system that does not use a similar crash test system likely contains undetected bugs that will lead to database corruption following a system crash or power failure."*

---

## 10. libSQL Improvements Over SQLite

libSQL (the fork by Turso) adds several relevant improvements:

### Virtual WAL Interface
libSQL introduces a pluggable `libsql_wal_methods` structure — a function pointer table for WAL operations. This allows custom WAL implementations (e.g., network-replicated WAL) to be injected without modifying the core.

### Bottomless Replication
Continuous backup to S3-compatible storage. WAL frames are batched and uploaded, providing crash safety beyond a single machine.

### Deterministic Simulation Testing (Limbo)
Turso's next-generation project (Limbo) is being built from scratch in Rust with deterministic simulation testing (partnering with Antithesis) to systematically verify crash safety at every possible failure point.

---

## 11. PGlite's Current State

### How PGlite Handles Filesystem (Node.js)

**NodeFS backend** (`packages/pglite/src/fs/nodefs.ts`):
- Uses Emscripten's NODEFS to directly mount a Node.js directory
- NODEFS is a thin passthrough — it maps Emscripten FS calls to Node.js `fs` calls
- **No crash safety logic exists in the NodeFS layer**

### What `close()` Does

```typescript
// pglite.ts lines 566-597
async close() {
  // 1. Run extension cleanup
  for (const closeFn of this.#extensionsClose) {
    await closeFn()
  }

  // 2. Send END to Postgres protocol
  await this.execProtocol(serialize.end())

  // 3. Shutdown Postgres (triggers WAL checkpoint internally)
  this.mod!._pgl_shutdown()

  // 4. Remove function pointers
  this.mod!.removeFunction(this.#pglite_read)
  this.mod!.removeFunction(this.#pglite_write)

  // 5. Close filesystem
  await this.fs!.closeFs()

  this.#closed = true
}
```

**If `close()` doesn't run** (kill -9, uncaught exception, power loss):
- Postgres's WAL is not checkpointed
- Emscripten's filesystem is not finalized
- On next open, `_pgl_initdb()` may find an inconsistent WAL and PANIC

### Sync After Queries

```typescript
// base.ts lines 306-307
if (!this.#inTransaction) {
  await this.syncToFs()
}
```

PGlite syncs to the filesystem after each query (unless `relaxedDurability` is enabled), but this is an **Emscripten-level sync**, not a Postgres WAL checkpoint. The Postgres WAL may still contain unflushed data.

### No File Locking

PGlite has no mechanism to prevent multiple instances from opening the same data directory. Issue #85 has been open since June 2024.

### No Signal Handlers

PGlite does not register any `process.on('SIGTERM')`, `process.on('SIGINT')`, or `process.on('beforeExit')` handlers.

### Known Corruption Issues

| Issue | Status | Problem |
|-------|--------|---------|
| #327 | Closed | PANIC: could not locate valid checkpoint record (fixed in v0.2.8 for graceful close only) |
| #645 | Open | syncfs not called on close — data may not persist |
| #794 | Open | Error recreating instance after close — NODEFS persistence issue |
| #884 | Open | RuntimeError: Aborted() on startup |
| #85 | Open | No lock on data directory — dual instances = corruption |
| #527 | Open | No panic handler during initdb — corrupted datadir left behind |
| #339 | Open | OOB memory access during checkpoint/shutdown |
| #819 | Open | WAL bloat — WAL files not cleaned up |

---

## 12. Gap Analysis: PGlite vs SQLite

| Crash Safety Feature | SQLite | PGlite | Gap Severity |
|---------------------|--------|--------|-------------|
| **Self-healing on open** | Yes — hot journal/WAL recovery | No — PANICs on corrupt WAL | **CRITICAL** |
| **Ordered fsync barriers** | Yes — journal/WAL fsynced before DB modified | Relies on Emscripten shim | **CRITICAL** |
| **File locking** | Yes — POSIX advisory locks | None | **HIGH** |
| **Atomic commit point** | Yes — journal deletion or WAL commit frame | Requires `close()` to run | **CRITICAL** |
| **Checksummed WAL frames** | Yes — cumulative checksums + salt validation | Postgres has its own, but recovery path is broken in PGlite | **HIGH** |
| **Crash simulation tests** | Yes — 17 test files, VFS-based fault injection | None | **HIGH** |
| **Signal handlers** | N/A (library, not server) | None (but PGlite IS used as a library in Node.js) | **MEDIUM** |
| **Periodic auto-checkpoint** | Yes — every ~1000 WAL pages | No — only on explicit `close()` | **HIGH** |
| **PRAGMA synchronous control** | Yes — OFF/NORMAL/FULL/EXTRA | Only `relaxedDurability` boolean | **LOW** |
| **Directory fsync** | Yes — ensures journal visibility after crash | No | **MEDIUM** |
| **Power-safe overwrite detection** | Yes — per-filesystem detection | No | **LOW** |

---

## 13. Recommendations for PGlite

### Priority 1: Self-Healing on Open (CRITICAL)

The most impactful change. When PGlite opens a data directory and finds an inconsistent WAL, it should **recover** rather than PANIC.

**What SQLite does**: Single sequential scan of WAL, validate checksums, stop at first invalid frame, rebuild index from valid frames only.

**What PGlite should do**:
- Before calling `_pgl_backend()`, check if the WAL state is consistent
- If not, attempt PostgreSQL's built-in crash recovery (WAL replay from last checkpoint)
- If recovery fails, provide a clear error message and optionally offer to reinitialize
- Consider exposing `pg_resetwal` functionality for last-resort recovery

### Priority 2: Periodic WAL Checkpointing (CRITICAL)

PGlite should periodically checkpoint the WAL to minimize data at risk.

**What SQLite does**: Auto-checkpoint every ~1000 WAL pages. `wal_autocheckpoint` pragma controls the threshold.

**What PGlite should do**:
- Run `CHECKPOINT` automatically on a timer (e.g., every 30-60 seconds)
- Run `CHECKPOINT` after every N queries
- Ensure the checkpoint actually fsyncs to disk, not just to Emscripten's buffer
- Make the interval configurable via options

### Priority 3: File Locking (HIGH)

Prevent multiple PGlite instances from opening the same data directory.

**What SQLite does**: POSIX advisory locks via `fcntl()` on specific byte ranges.

**What PGlite should do**:
- Create a lock file (e.g., `postmaster.pid` like real Postgres) with `O_EXCL`
- Or use `flock()` on the data directory
- Throw a clear error: "This data directory is already in use by another PGlite instance"
- Clean up stale lock files on open (check if the PID in the lock file is still alive)

### Priority 4: Signal Handlers for Node.js (MEDIUM)

Register cleanup handlers for catchable signals.

**What PGlite should do**:
```typescript
// In the PGlite constructor, when running in Node.js:
if (typeof process !== 'undefined' && process.on) {
  const cleanup = async () => {
    await this.close()
    process.exit(0)
  }
  process.on('SIGINT', cleanup)
  process.on('SIGTERM', cleanup)
  process.on('beforeExit', cleanup)
}
```

**Note**: This does NOT help with `kill -9` (SIGKILL), but it covers `Ctrl+C`, `kill`, and normal process exit. The self-healing-on-open mechanism (Priority 1) is what handles `kill -9`.

### Priority 5: fsync Guarantees in NodeFS (MEDIUM)

Ensure that when PGlite syncs, data actually reaches the disk.

**What SQLite does**: Calls `fsync()`/`fdatasync()` directly, with `F_FULLFSYNC` on macOS.

**What PGlite should do**:
- After critical writes (WAL checkpoint, after commits), call `fs.fsyncSync()` on the relevant files
- On macOS, consider using `F_FULLFSYNC` via native bindings for power-loss safety
- The current NODEFS passthrough does not guarantee fsync ordering

### Priority 6: Crash Simulation Tests (HIGH, but longer-term)

**What SQLite does**: Custom VFS that injects I/O failures and simulates crashes at every possible point, then verifies database integrity.

**What PGlite should do**:
- Create a test harness that:
  1. Opens a PGlite database on disk
  2. Runs a series of writes
  3. Kills the process at random points (via child process `kill -9`)
  4. Reopens the database and verifies integrity
  5. Repeats thousands of times
- This would catch regressions in crash safety and validate the self-healing mechanism

### Priority 7: Graceful Degradation on initdb Failure (MEDIUM)

**What PGlite should do**:
- If `_pgl_initdb()` fails partway through, clean up the data directory
- Don't leave a half-initialized database that will fail on every subsequent open
- Issue #527 tracks this

---

## Summary

SQLite's crash safety is not a single feature — it's a **pervasive architectural property** that touches every layer of the system. The key mechanisms are:

1. **Write ordering enforced by fsync barriers** — the journal/WAL is always durable before the database is modified
2. **Self-validating WAL with checksums and salts** — recovery can determine exactly which transactions committed
3. **Automatic recovery on open** — no external tools needed, no manual intervention
4. **File locking** — prevents concurrent access corruption entirely
5. **Exhaustive crash simulation testing** — the only way to have confidence in crash safety

For PGlite to achieve similar robustness, the most critical investment is **self-healing on open** (Priority 1). If PGlite can recover from a dirty WAL instead of panicking, `kill -9` becomes survivable. Combined with periodic checkpointing (Priority 2) and file locking (Priority 3), PGlite would cover the vast majority of corruption scenarios reported in its issue tracker.

---

## References

### SQLite Documentation
- [Atomic Commit In SQLite](https://sqlite.org/atomiccommit.html)
- [Write-Ahead Logging](https://sqlite.org/wal.html)
- [WAL-mode File Format](https://sqlite.org/walformat.html)
- [File Locking And Concurrency](https://sqlite.org/lockingv3.html)
- [Powersafe Overwrite](https://sqlite.org/psow.html)
- [How SQLite Is Tested](https://sqlite.org/testing.html)
- [How To Corrupt An SQLite Database](https://sqlite.org/howtocorrupt.html)

### SQLite Source Files Analyzed
- `wal.c` — WAL implementation (4621 lines)
- `pager.c` — Pager/journal implementation (7834 lines)
- `os_unix.c` — Unix VFS with fsync/locking (8589 lines)
- `crashtest1.c` — C-level crash simulation

### PGlite Source Files Analyzed
- `packages/pglite/src/pglite.ts` — Main PGlite class
- `packages/pglite/src/base.ts` — Base class with sync logic
- `packages/pglite/src/fs/nodefs.ts` — Node.js filesystem backend
- `packages/pglite/src/fs/opfs-ahp.ts` — OPFS backend (browser, has WAL)
- `packages/pglite/src/fs/idbfs.ts` — IndexedDB backend

### PGlite GitHub Issues
- #85: Add lock to prevent double-open
- #327: PANIC on corrupt checkpoint record
- #339: OOB memory access during checkpoint
- #527: Missing initdb panic handler
- #645: Call syncfs on close
- #794: Error recreating instance after shutdown
- #819: WAL bloat
- #884: RuntimeError: Aborted()

### External Resources
- [Files are hard — Dan Luu](https://danluu.com/file-consistency/)
- [Fsyncgate — Dan Luu](https://danluu.com/fsyncgate/)
- [SQLite on macOS: Not ACID compliant — BonsaiDb](https://bonsaidb.io/blog/acid-on-apple/)
- [PostgreSQL Recovery Internals — Cybertec](https://www.cybertec-postgresql.com/en/postgresql-recovery-internals/)
