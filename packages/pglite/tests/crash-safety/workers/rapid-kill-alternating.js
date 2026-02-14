/**
 * Worker: Alternating heavy/light writes
 *
 * If HEAVY_MODE=1, inserts 1000 rows with 1KB payloads.
 * If HEAVY_MODE=0, inserts 1 row with tiny payload.
 * The inconsistent WAL sizes may trip up recovery.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   HEAVY_MODE      - "1" for heavy writes, "0" for light writes
 *   CYCLE_NUM       - cycle number for tracking
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const heavyMode = process.env.HEAVY_MODE === '1'
const cycleNum = parseInt(process.env.CYCLE_NUM || '0', 10)

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  await db.query(`
    CREATE TABLE IF NOT EXISTS alternating_test (
      id SERIAL PRIMARY KEY,
      cycle INTEGER NOT NULL,
      heavy BOOLEAN NOT NULL,
      payload TEXT NOT NULL
    )
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_alternating_cycle ON alternating_test (cycle)
  `)
  process.send('schema-ready')

  if (heavyMode) {
    // Heavy: 1000 rows with 1KB payloads
    for (let i = 0; i < 1000; i++) {
      await db.query(
        `INSERT INTO alternating_test (cycle, heavy, payload) VALUES ($1, $2, $3)`,
        [cycleNum, true, `heavy-${cycleNum}-${i}-${'H'.repeat(800)}`]
      )
      if (i % 100 === 0) {
        process.send(`heavy-progress:${i}`)
      }
    }
    process.send('heavy-writes-done')
  } else {
    // Light: just 1 tiny row
    await db.query(
      `INSERT INTO alternating_test (cycle, heavy, payload) VALUES ($1, $2, $3)`,
      [cycleNum, false, 'light']
    )
    process.send('light-write-done')
  }

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Alternating worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
