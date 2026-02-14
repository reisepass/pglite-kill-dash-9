/**
 * Worker: WAL bloat with no checkpoint
 *
 * Performs heavy mixed DML operations (INSERT, UPDATE, DELETE) in rapid
 * succession without ever calling close() or CHECKPOINT. The parent process
 * will SIGKILL this worker at various points to simulate a crash.
 *
 * Each cycle adds 50 rows, updates random rows, and deletes some rows.
 * This generates a huge amount of WAL without any checkpoint to compact it.
 * Over 30+ cycles this creates extreme WAL pressure that tests whether
 * PGlite's recovery can handle the accumulated, never-checkpointed state.
 *
 * Additionally, this worker creates indexes and does schema modifications
 * mid-stream to make the WAL entries more complex (not just simple tuple ops).
 *
 * Environment variables:
 *   PGLITE_DATA_DIR  - path to the data directory
 *   INNER_CYCLES     - number of DML cycles to run inside this process
 *   START_CYCLE      - the starting cycle number (for tracking across kills)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const innerCycles = parseInt(process.env.INNER_CYCLES || '10', 10)
const startCycle = parseInt(process.env.START_CYCLE || '0', 10)

// Simple deterministic pseudo-random (no need for crypto randomness)
let seed = startCycle * 1000 + 42
function nextRand() {
  seed = (seed * 1103515245 + 12345) & 0x7fffffff
  return seed
}

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  // Always create schema idempotently — previous cycles may have been killed
  // before schema creation completed, so we must ensure tables exist.
  await db.query(`
    CREATE TABLE IF NOT EXISTS wal_stress (
      id SERIAL PRIMARY KEY,
      cycle INTEGER NOT NULL,
      batch INTEGER NOT NULL,
      kind TEXT NOT NULL,
      value TEXT NOT NULL,
      counter INTEGER DEFAULT 0,
      updated_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_wal_stress_cycle ON wal_stress (cycle)
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_wal_stress_kind ON wal_stress (kind)
  `)
  await db.query(`
    CREATE TABLE IF NOT EXISTS wal_stress_log (
      id SERIAL PRIMARY KEY,
      cycle INTEGER NOT NULL,
      operation TEXT NOT NULL,
      row_count INTEGER,
      logged_at TIMESTAMP DEFAULT NOW()
    )
  `)
  process.send('schema-created')

  // Run inner cycles of heavy mixed DML
  for (let c = 0; c < innerCycles; c++) {
    const cycleNum = startCycle + c
    process.send(`cycle-start:${cycleNum}`)

    // ---- INSERT phase: 50 rows with ~1KB payloads ----
    const padding = 'W'.repeat(800)
    for (let i = 0; i < 50; i++) {
      const kind = ['alpha', 'beta', 'gamma', 'delta'][nextRand() % 4]
      await db.query(
        `INSERT INTO wal_stress (cycle, batch, kind, value, counter)
         VALUES ($1, $2, $3, $4, $5)`,
        [cycleNum, i, kind, `c${cycleNum}-b${i}-${kind}-${padding}`, nextRand() % 10000]
      )
    }
    process.send(`inserts-done:${cycleNum}`)

    // ---- UPDATE phase: update random subsets ----
    // Update by kind (touches many rows across multiple pages)
    const kinds = ['alpha', 'beta', 'gamma', 'delta']
    const targetKind = kinds[nextRand() % 4]
    await db.query(
      `UPDATE wal_stress
       SET counter = counter + $1, value = value || $2, updated_at = NOW()
       WHERE kind = $3`,
      [nextRand() % 100, `-upd${cycleNum}`, targetKind]
    )

    // Update specific rows by cycle (generates different WAL patterns)
    const updateCycle = nextRand() % (cycleNum + 1)
    await db.query(
      `UPDATE wal_stress
       SET counter = counter + 1
       WHERE cycle = $1 AND batch < 25`,
      [updateCycle]
    )

    // Scatter update: update every Nth row by ID
    const step = (nextRand() % 5) + 2
    await db.query(
      `UPDATE wal_stress
       SET value = LEFT(value, 200) || $1
       WHERE id % $2 = 0`,
      [`-scatter${cycleNum}`, step]
    )

    process.send(`updates-done:${cycleNum}`)

    // ---- DELETE phase: delete some rows to create holes ----
    // Delete old rows from a random previous cycle (creates page holes)
    if (cycleNum > 2) {
      const deleteCycle = nextRand() % Math.max(1, cycleNum - 1)
      const deleteLimit = (nextRand() % 15) + 5
      await db.query(
        `DELETE FROM wal_stress
         WHERE id IN (
           SELECT id FROM wal_stress
           WHERE cycle = $1 AND batch >= $2
           ORDER BY id
           LIMIT $3
         )`,
        [deleteCycle, 30, deleteLimit]
      )
    }

    // Delete rows with specific kind to thin out data
    if (cycleNum % 3 === 0) {
      await db.query(
        `DELETE FROM wal_stress
         WHERE kind = $1 AND cycle < $2 AND batch > 40`,
        [kinds[nextRand() % 4], cycleNum]
      )
    }

    process.send(`deletes-done:${cycleNum}`)

    // ---- LOG phase: record what we did (more WAL entries) ----
    await db.query(
      `INSERT INTO wal_stress_log (cycle, operation, row_count)
       VALUES ($1, $2, (SELECT count(*) FROM wal_stress))`,
      [cycleNum, 'full-cycle']
    )

    // ---- Occasional complex operations to diversify WAL ----
    if (cycleNum % 5 === 0 && cycleNum > 0) {
      // Add a column (DDL in WAL)
      const colName = `extra_${cycleNum}`
      await db.query(`
        ALTER TABLE wal_stress ADD COLUMN IF NOT EXISTS ${colName} TEXT DEFAULT NULL
      `)

      // Create a partial index (more complex WAL)
      await db.query(`
        CREATE INDEX IF NOT EXISTS idx_wal_stress_c${cycleNum}
        ON wal_stress (counter)
        WHERE cycle = ${cycleNum}
      `)

      process.send(`ddl-done:${cycleNum}`)
    }

    // ---- Large batch UPDATE to really bloat the WAL ----
    if (cycleNum % 4 === 0) {
      await db.query(
        `UPDATE wal_stress
         SET counter = counter + $1
         WHERE cycle >= $2`,
        [1, Math.max(0, cycleNum - 3)]
      )
    }

    process.send(`cycle-done:${cycleNum}`)
  }

  process.send('all-cycles-done')

  // Intentionally do NOT call db.close() or CHECKPOINT.
  // Keep the process alive so the parent can SIGKILL it.
  // Using setInterval to keep the event loop alive (a bare unresolved
  // Promise doesn't prevent Node.js from exiting).
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`WAL bloat worker error:`, err)
  try {
    process.send(`fatal:${err.message}`)
  } catch (_) {}
  process.exit(1)
})
