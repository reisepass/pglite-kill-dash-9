/**
 * Worker: Minimum kills to corrupt
 *
 * Does heavy DML writes (similar to wal-bloat worker) but in a single
 * aggressive burst. Designed to be killed after writes but before close().
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE_NUM       - cycle number for tracking
 *   ROW_COUNT       - number of rows to insert (default 100)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycleNum = parseInt(process.env.CYCLE_NUM || '0', 10)
const rowCount = parseInt(process.env.ROW_COUNT || '100', 10)

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  // Create schema on first cycle
  if (cycleNum === 0) {
    await db.query(`
      CREATE TABLE IF NOT EXISTS min_kill_test (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        batch INTEGER NOT NULL,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL,
        counter INTEGER DEFAULT 0
      )
    `)
    await db.query(`CREATE INDEX IF NOT EXISTS idx_min_kill_cycle ON min_kill_test (cycle)`)
    await db.query(`CREATE INDEX IF NOT EXISTS idx_min_kill_kind ON min_kill_test (kind)`)
    process.send('schema-created')
  }

  // INSERT phase: heavy rows
  const kinds = ['alpha', 'beta', 'gamma', 'delta']
  for (let i = 0; i < rowCount; i++) {
    const kind = kinds[i % 4]
    await db.query(
      `INSERT INTO min_kill_test (cycle, batch, kind, payload, counter) VALUES ($1, $2, $3, $4, $5)`,
      [cycleNum, i, kind, `c${cycleNum}-b${i}-${kind}-${'M'.repeat(800)}`, i * cycleNum]
    )
  }
  process.send('inserts-done')

  // UPDATE phase: update all rows from this cycle
  await db.query(
    `UPDATE min_kill_test SET counter = counter + 1, payload = payload || '-updated' WHERE cycle = $1`,
    [cycleNum]
  )
  process.send('updates-done')

  // DELETE phase: remove some from previous cycles
  if (cycleNum > 0) {
    await db.query(
      `DELETE FROM min_kill_test WHERE cycle = $1 AND batch > $2`,
      [cycleNum - 1, rowCount - 10]
    )
  }
  process.send('deletes-done')

  // Cross-cycle update to generate complex WAL
  await db.query(`UPDATE min_kill_test SET counter = counter + 1 WHERE batch < 10`)
  process.send('cross-update-done')

  process.send('all-done')

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Minimum kill worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
