# PGlite Crash Safety Test Results

## Overview

Comprehensive crash safety testing of PGlite's NodeFS (on-disk) backend to identify and reproduce file corruption scenarios. These tests simulate real-world crash conditions that PGlite users encounter during local development — particularly `kill -9`, dev server restarts (nodemon, vite HMR), and power loss.

**69 total test cases across 21 test files. 63 pass, 6 fail (proving corruption).**

## Summary of Findings

### What CORRUPTS PGlite (6 failing tests)

| Scenario | Test File | Corruption Trigger | Error |
|----------|-----------|-------------------|-------|
| HMR double-instance replacement | `hmr-double-instance.test.js` | Two PGlite instances open same data dir, both write, SIGKILL after 3-4 cycles | `Aborted()` |
| HMR rapid instance swaps | `hmr-double-instance.test.js` | Rapid new-instance-before-old-dies cycling | `Aborted()` |
| WAL bloat burst (15 rapid kills) | `wal-bloat-no-checkpoint.test.js` | 15 extremely rapid open-write-SIGKILL cycles, no CHECKPOINT | `Aborted()` |
| Dev-server restart simulation | `dev-server-cycle.test.js` | 20+ restart cycles with varying operations and kill timings | `Aborted()` |
| Double-open same data dir | `double-open.test.js` | Two instances on same dir simultaneously | `Aborted()` |
| Rapid restart cycles | `rapid-restart.test.js` | Multiple rapid dirty shutdowns | `Aborted()` |

### Root Causes Identified

**1. No file locking (GitHub issue #85)**
PGlite does not lock the data directory. Multiple instances can open the same directory simultaneously, leading to WAL corruption as both write to the same files. This is the #1 cause of corruption in dev environments where HMR or file-watch restarts create overlapping instances.

**2. Kill during first-time initialization**
Killing PGlite during its first-ever `initdb` on a new data directory leaves a partially-initialized directory that can never be opened again. The only recovery is deleting the data directory entirely.

**3. No crash recovery for rapid kill cycles**
Extremely rapid cycles of open-write-SIGKILL without any CHECKPOINT accumulate WAL state that eventually becomes unrecoverable. This mirrors the real-world pattern of a developer repeatedly restarting their dev server with `kill -9`.

### What PGlite SURVIVES (63 passing tests)

PGlite's PostgreSQL WAL recovery is remarkably robust for single-instance crash scenarios:

| Scenario | Result |
|----------|--------|
| Kill during bulk INSERT | Recovers, data consistent |
| Kill during open transaction | Recovers, transaction rolled back |
| Kill during ALTER TABLE | Recovers, schema consistent |
| Kill during batch load | Recovers |
| Kill during index creation | Recovers |
| Kill during VACUUM | Recovers |
| Kill during close() | Recovers |
| Kill during WAL recovery | Recovers (re-replays WAL) |
| Kill during CHECKPOINT (36 confirmed kills) | Recovers every time |
| Schema changes across kills | Recovers |
| Massive WAL (27MB) then kill | Recovers |
| 25 kill-during-init cycles (already-initialized DB) | Recovers |
| 20 kill-during-recovery cycles | Recovers |
| 20 alternating heavy/light write kills | Recovers |
| 20 schema DDL kill cycles | Recovers |
| 30 mid-write kills (48MB WAL, 15K+ rows) | Recovers |
| WAL tail truncation (simulated power loss) | Recovers |
| WAL segment zeroing | Recovers |
| WAL garbage injection | Recovers |
| WAL segment deletion (middle) | Recovers |
| WAL segment duplication | Recovers |

### WAL File Manipulation Results (17 tests)

Simulating power-loss scenarios by manipulating WAL files after a kill:

| Manipulation | Result | Notes |
|-------------|--------|-------|
| Truncate first WAL to 50% | **CORRUPTED** | `Aborted()` |
| Corrupt all WAL headers | **CORRUPTED** | `Aborted()` |
| Delete all WAL segments | **CORRUPTED** | `Aborted()` |
| Corrupt heap + truncate WAL | **CORRUPTED** | `invalid page in block 0` |
| Swap two WAL segments | **CORRUPTED** | `Aborted()` |
| Corrupt pg_control LSN | **CORRUPTED** | `unreachable` |
| Truncate heap files to 0 | **CORRUPTED** | `could not read blocks` |
| Total destruction (all files) | **CORRUPTED** | `unreachable` |
| Zero middle WAL segment | Recovered | 100 rows intact |
| Delete middle WAL segment | Recovered | 100 rows intact |
| Truncate pg_control to half | **Not detected** | Opens successfully (vulnerability) |
| Truncate data file to half | **Not detected** | Queries succeed (vulnerability) |

### Overlapping Instances Results (5 tests)

All scenarios corrupt PGlite — the lack of file locking is the most exploitable vulnerability:

| Scenario | Result |
|----------|--------|
| 3 simultaneous instances | WASM abort on first cycle |
| Staggered overlap (A writes, B opens while A alive) | Corruption by cycle 3 |
| DDL vs DML collision (separate processes) | Worker crashes with exit(1) |
| 10 instances opened without closing previous | WASM abort after 4th instance |
| Recovery overlap (2 instances recovering simultaneously) | Abort on corrupted dir |

### Kill During CHECKPOINT Results (5 tests)

PGlite is fully resilient to checkpoint kills. PostgreSQL's WAL remains authoritative until checkpoint completion is recorded in pg_control. Killing mid-checkpoint simply triggers a full WAL replay on next startup.

- 36 confirmed kills during active CHECKPOINT across 40 attempts
- 0 corruptions
- All integrity checks pass (table scans, index scans, cross-table consistency)

## Vulnerability Classification

### Critical (causes unrecoverable corruption in normal dev workflows)

1. **No data directory locking** — Any scenario where two PGlite instances touch the same directory simultaneously (HMR, nodemon restart, accidental double-open) results in corruption. This is the most common real-world corruption vector.

2. **Kill during first-time init** — If PGlite is killed before `initdb` completes on a brand-new data directory, the directory is permanently corrupted. Only fix is `rm -rf`.

### Moderate (requires specific conditions)

3. **Rapid kill cycling without checkpoint** — 15+ rapid open-write-kill cycles without CHECKPOINT can accumulate unrecoverable WAL state. This requires sustained rapid kills but matches aggressive dev server restart patterns.

### Low (requires filesystem-level failures)

4. **pg_control truncation not detected** — A partially-truncated pg_control file is not detected on open. This could lead to silent data loss after a power failure that corrupts pg_control.

5. **Data file truncation not detected** — A truncated data file passes basic open/query checks. PostgreSQL only reads pages on demand, so missing pages aren't detected until accessed.

## Recommendations for PGlite

1. **Add data directory locking** (addresses #1) — Use `flock()` or a lockfile to prevent multiple instances from opening the same directory.

2. **Add init-in-progress marker** (addresses #2) — Write a marker file at the start of `initdb`, remove it on completion. On open, if the marker exists, delete the directory and re-initialize.

3. **Add periodic automatic CHECKPOINT** (addresses #3) — Run `CHECKPOINT` on an interval (e.g., every 30s) to bound the WAL replay window.

4. **Add pg_control integrity check** (addresses #4) — Verify pg_control size and CRC on open.

5. **Ship pg_resetwal** (general recovery) — Include `pg_resetwal` as a recovery tool for corrupted databases.

## Running the Tests

```bash
cd packages/pglite

# Run all crash safety tests
npx vitest run tests/crash-safety/ --no-file-parallelism

# Run a specific test
npx vitest run tests/crash-safety/overlapping-instances.test.js

# Retain data directories for debugging
RETAIN_DATA=1 npx vitest run tests/crash-safety/wal-truncation.test.js
```

## Test File Index

### Test Harness
- `harness.js` — Shared utilities: `crashTest()`, `tryOpen()`, `verifyIntegrity()`, `cleanupDataDir()`

### Corruption Tests (expect failure = prove corruption exists)
- `hmr-double-instance.test.js` — HMR-style instance replacement
- `wal-bloat-no-checkpoint.test.js` — WAL bloat from rapid kills
- `overlapping-instances.test.js` — Multiple concurrent instances (5 scenarios)
- `rapid-kill-cycles.test.js` — Ultra-rapid kill cycles (13 scenarios)
- `wal-truncation.test.js` — Simulated power-loss WAL corruption (11 scenarios)
- `wal-manipulation.test.js` — Direct WAL/data file manipulation (17 scenarios)
- `double-open.test.js` — Two instances on same directory
- `dev-server-cycle.test.js` — Dev server restart simulation
- `rapid-restart.test.js` — Rapid dirty shutdowns

### Resilience Tests (expect success = prove PGlite recovers)
- `kill-during-insert.test.js` — Kill during bulk INSERT
- `kill-during-transaction.test.js` — Kill during open transaction
- `kill-during-alter-table.test.js` — Kill during DDL
- `kill-during-batch-load.test.js` — Kill during batch load
- `kill-during-index-creation.test.js` — Kill during index build
- `kill-during-vacuum.test.js` — Kill during VACUUM
- `kill-during-close.test.js` — Kill during close()
- `kill-during-recovery.test.js` — Kill during WAL recovery
- `kill-during-checkpoint.test.js` — Kill during CHECKPOINT (5 scenarios)
- `massive-wal-crash.test.js` — Large WAL then kill
- `schema-change-crash.test.js` — Schema changes then kill
- `concurrent-writers.test.js` — Concurrent writers then kill
