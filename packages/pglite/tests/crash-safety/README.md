# PGlite Crash Safety Test Suite

Reproducible test scenarios that verify PGlite's resilience to `kill -9` (SIGKILL), dirty shutdowns, and concurrent access corruption. Each test spawns a child process performing PGlite operations, kills it at a specific moment, then verifies the database can be reopened and is consistent.

## Running the Tests

```bash
# Run all crash safety tests
pnpm vitest run tests/crash-safety/ --reporter=verbose

# Run a single scenario
pnpm vitest run tests/crash-safety/kill-during-insert.test.js

# Keep data directories for debugging (not cleaned up after test)
RETAIN_DATA=1 pnpm vitest run tests/crash-safety/
```

> **Note:** Do not use `--no-file-parallelism` — PGlite's WASM module conflicts with vitest's single-worker mode.

## Architecture

```
tests/crash-safety/
├── harness.js                          # Shared test infrastructure
├── README.md                           # This file
├── workers/                            # Child process scripts (one per scenario)
│   ├── kill-during-insert.js
│   ├── kill-during-transaction.js
│   ├── kill-during-index-creation.js
│   ├── kill-during-alter-table.js
│   ├── kill-during-vacuum.js
│   ├── kill-during-batch-load.js
│   ├── rapid-restart-worker.js
│   └── double-open-worker.js
├── kill-during-insert.test.js          # Test files (one per scenario)
├── kill-during-transaction.test.js
├── kill-during-index-creation.test.js
├── kill-during-alter-table.test.js
├── kill-during-vacuum.test.js
├── kill-during-batch-load.test.js
├── rapid-restart.test.js
└── double-open.test.js
```

### How It Works

Each test follows the same pattern:

1. **Worker script** — A standalone Node.js script that creates a PGlite instance on a data directory (passed via `PGLITE_DATA_DIR` env var), performs database operations, and sends IPC messages to the parent via `process.send()` to signal progress.

2. **Test file** — Uses vitest. Calls `crashTest()` from the harness to spawn the worker as a child process via `fork()`. The harness kills the child with `SIGKILL` either after a timer or when a specific IPC message is received (e.g., `'inserting'`, `'in-transaction'`).

3. **Verification** — After the kill, the test reopens PGlite on the same data directory and checks:
   - The database opens without error (no PANIC, no hang)
   - Basic queries succeed (`SELECT 1`)
   - All user tables are scannable
   - Data is consistent (committed rows present, uncommitted rows absent)

4. **Cleanup** — Each test uses a unique `/tmp/pglite-crash-*` directory and removes it in `afterAll`, unless `RETAIN_DATA=1` is set.

## Harness API (`harness.js`)

### `crashTest(options)`

Spawns a child process and kills it.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dataDir` | string | required | Path to PGlite data directory |
| `workerScript` | string | required | Path to the worker `.js` file |
| `killAfterMs` | number | `500` | Delay before sending kill signal |
| `signal` | string | `'SIGKILL'` | Signal to send (usually SIGKILL) |
| `killOnMessage` | string | `null` | Kill when worker sends this IPC message instead of using timer |
| `env` | object | `{}` | Extra environment variables for the child |

Returns: `{ workerKilled, workerError, workerMessages, workerExitCode, workerSignal, stdout, stderr }`

### `tryOpen(dataDir, timeoutMs?)`

Attempts to open a PGlite instance on a potentially corrupted data directory. Includes a timeout (default 15s) to handle cases where a corrupted database hangs forever during initialization.

Returns: `{ success, db, error }`

### `verifyIntegrity(db)`

Runs integrity checks against an open PGlite instance: basic query, table scan, index scan.

Returns: `{ intact, issues }`

### `cleanupDataDir(dataDir)`

Removes a test data directory recursively.

### `testDataDir(scenarioName)`

Generates a unique `/tmp/pglite-crash-<name>-<timestamp>-<rand>` path.

## Test Scenarios

### 1. Kill During Bulk INSERT

**File:** `kill-during-insert.test.js`

Worker creates a table and inserts 500 rows one at a time (each INSERT is its own implicit transaction, not wrapped in BEGIN/COMMIT). The parent kills the process after the worker reports it has started inserting.

**What it tests:** Partial row insertion — some rows committed, some not. Can PGlite recover and serve the committed rows?

**Verification:** Database opens, row count is >= 0 and is a valid integer, table is scannable.

### 2. Kill During Explicit Transaction

**File:** `kill-during-transaction.test.js`

Worker creates a table, inserts 10 baseline rows (committed), then opens a BEGIN block and performs INSERTs, UPDATEs, and DELETEs inside the transaction without committing. Parent kills the process while the transaction is open.

**What it tests:** Uncommitted transaction rollback. The baseline data should survive intact, and the in-flight transaction's changes should be fully rolled back.

**Verification:**
- All 10 baseline rows exist with original values
- Zero transaction rows present (`txn-%` pattern)
- UPDATE changes reverted (values match original `idx * 10`)
- DELETE changes reverted (baseline-1 and baseline-2 still exist)

### 3. Kill During CREATE INDEX

**File:** `kill-during-index-creation.test.js`

Worker creates a table, inserts 1000+ rows to make index creation non-trivial, then starts `CREATE INDEX`. Parent kills the process right when index creation begins.

**What it tests:** Interrupted DDL that modifies internal catalog structures and writes index pages. Can PGlite recover without a half-built orphaned index corrupting the catalog?

**Verification:** Database opens, table data is intact and scannable. Index may or may not exist (either outcome is acceptable as long as the database is consistent).

### 4. Kill During ALTER TABLE

**File:** `kill-during-alter-table.test.js`

Worker creates a table with data, then performs a sequence of ALTER TABLE operations: ADD COLUMN, ADD COLUMN with DEFAULT, ADD CONSTRAINT. Parent kills during these operations.

**What it tests:** Interrupted schema modifications that touch the system catalog (`pg_attribute`, `pg_class`, `pg_constraint`).

**Verification:** Database opens, original table and data are accessible, schema is queryable.

### 5. Kill During VACUUM FULL

**File:** `kill-during-vacuum.test.js`

Worker creates a table, inserts many rows, deletes roughly half (creating dead tuples), then runs `VACUUM FULL`. Parent kills during the vacuum.

**What it tests:** VACUUM FULL rewrites the entire table file. Interrupting it mid-rewrite could leave the table in an inconsistent state or orphan the old/new file.

**Verification:** Database opens, surviving rows are accessible, count matches expected live rows.

### 6. Kill During Large Batch Load

**File:** `kill-during-batch-load.test.js`

Worker creates a table, inserts 10 baseline rows (committed), then constructs a single INSERT statement with 5000+ VALUES rows and executes it. Parent kills during the batch.

**What it tests:** A large single-statement write that generates significant WAL volume. Tests whether partial WAL writes from one large statement corrupt the database.

**Verification:**
- Database opens
- Baseline 10 rows are present
- Total count is either 10 (batch didn't commit) or 10 + batch size (batch committed) — never an intermediate value

### 7. Rapid Restart Cycle

**File:** `rapid-restart.test.js`

Runs 6 cycles of: open PGlite → do some work → SIGKILL. Each cycle inserts a row tagged with the cycle number. Some cycles are killed quickly (300ms timer), others are killed after operations complete.

**What it tests:** Cumulative corruption from repeated dirty shutdowns. Each restart must recover from the previous crash before doing new work, stacking recovery on top of recovery.

**Verification:** After all 6 cycles, database opens, table exists, row count is valid (at least 1 from cycle 0), all rows are readable.

### 8. Concurrent Double-Open

**File:** `double-open.test.js`

Creates a database, then spawns TWO child processes that both open PGlite on the same data directory simultaneously. Both instances read and write concurrently for 3 seconds, then both are killed.

**What it tests:** PGlite has no file locking (GitHub issue #85). Two instances accessing the same data directory can corrupt it. This test documents that gap.

**What it will test in the future:** Once PGlite implements file locking, the second instance should fail to open with a clear error message, and the first instance's data should remain intact.

**Verification:** Database can be reopened and queried after both instances are killed. (Currently passes because timing doesn't always trigger conflict, but proves the code path.)

## Current Results

As of the initial implementation, all 8 tests **pass**. This means PGlite's PostgreSQL WAL recovery handles most single-process crash scenarios correctly. The corruption the user experiences in practice is likely caused by:

- Double-open (two instances, no locking) — timing-dependent, not always reproducible in a single run
- Kill during internal WAL checkpoint (happens inside `close()`, which our tests skip by design)
- Accumulated WAL bloat from many dirty shutdowns without checkpointing
- Specific data volume thresholds that push the WAL past recovery limits

## Future Improvements

To make these tests catch real corruption more reliably:

1. **Stress loop mode** — run each scenario 100+ times to catch intermittent timing-dependent failures
2. **Kill during checkpoint** — requires a PGlite hook to signal when an internal checkpoint is running
3. **WAL bloat then kill** — fill the WAL with large volume before killing
4. **Aggressive double-open** — longer concurrent access windows with heavier write loads
5. **Crash simulation VFS** — like SQLite's test infrastructure, inject I/O failures at specific points in the filesystem layer
