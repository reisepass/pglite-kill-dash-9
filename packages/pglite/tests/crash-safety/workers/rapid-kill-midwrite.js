/**
 * Worker: Kill mid-write with continuous streaming writes
 *
 * This worker continuously writes rows in a tight loop, sending a message
 * after every single row. The parent kills it mid-stream. This ensures the
 * kill happens during active write operations, not between them.
 *
 * It also does large multi-row UPDATEs and DELETEs mid-stream to create
 * complex partial WAL states.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE_NUM       - cycle number for tracking
 *   MODE            - "setup" for first cycle, "stress" for subsequent
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycleNum = parseInt(process.env.CYCLE_NUM || '0', 10)
const mode = process.env.MODE || 'stress'

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  // Create schema on setup cycle
  if (mode === 'setup' || cycleNum === 0) {
    await db.query(`
      CREATE TABLE IF NOT EXISTS midwrite_test (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        kind TEXT NOT NULL,
        payload TEXT NOT NULL,
        counter INTEGER DEFAULT 0,
        big_blob TEXT DEFAULT NULL
      )
    `)
    await db.query(`CREATE INDEX IF NOT EXISTS idx_midwrite_cycle ON midwrite_test (cycle)`)
    await db.query(`CREATE INDEX IF NOT EXISTS idx_midwrite_kind ON midwrite_test (kind)`)
    await db.query(`CREATE INDEX IF NOT EXISTS idx_midwrite_counter ON midwrite_test (counter)`)
    process.send('schema-created')
  }

  const kinds = ['alpha', 'beta', 'gamma', 'delta']
  const bigPayload = 'X'.repeat(2000) // 2KB per row

  // Continuous streaming writes - send message after every row
  // The parent can kill at ANY point during this stream
  let seq = 0

  // Phase 1: Rapid inserts with large payloads
  for (let i = 0; i < 500; i++) {
    const kind = kinds[i % 4]
    await db.query(
      `INSERT INTO midwrite_test (cycle, seq, kind, payload, counter, big_blob)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [cycleNum, seq++, kind, `c${cycleNum}-s${seq}-${bigPayload}`, i, bigPayload]
    )
    // Signal every 10 rows to give kill opportunities
    if (i % 10 === 0) {
      process.send(`row:${i}`)
    }
  }
  process.send('inserts-done')

  // Phase 2: Large UPDATE touching many rows across all cycles
  // This generates a LOT of WAL because it modifies many heap pages
  await db.query(
    `UPDATE midwrite_test SET counter = counter + $1, payload = payload || $2 WHERE kind = $3`,
    [cycleNum, `-upd${cycleNum}`, kinds[cycleNum % 4]]
  )
  process.send('big-update-done')

  // Phase 3: Another round of inserts (doubles the data)
  for (let i = 0; i < 500; i++) {
    const kind = kinds[(i + 1) % 4]
    await db.query(
      `INSERT INTO midwrite_test (cycle, seq, kind, payload, counter, big_blob)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [cycleNum, seq++, kind, `c${cycleNum}-s${seq}-round2-${bigPayload}`, i + 1000, bigPayload]
    )
    if (i % 10 === 0) {
      process.send(`row2:${i}`)
    }
  }
  process.send('inserts2-done')

  // Phase 4: Massive cross-cycle update
  await db.query(`UPDATE midwrite_test SET counter = counter + 1`)
  process.send('mass-update-done')

  // Phase 5: Deletes with subquery (complex WAL)
  if (cycleNum > 0) {
    await db.query(
      `DELETE FROM midwrite_test WHERE id IN (
        SELECT id FROM midwrite_test WHERE cycle < $1 AND seq > 400 ORDER BY id LIMIT 200
      )`,
      [cycleNum]
    )
  }
  process.send('deletes-done')

  process.send('all-done')

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Midwrite worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
