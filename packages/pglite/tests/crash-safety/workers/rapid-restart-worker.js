/**
 * Worker: Rapid restart cycle
 *
 * Simulates rapidly starting PGlite, performing a few operations, then
 * being killed. On the first cycle it creates a table, and on subsequent
 * cycles it inserts rows tagged with the cycle number.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE_NUMBER    - which cycle this is (0-based)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycleNumber = parseInt(process.env.CYCLE_NUMBER, 10)

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  if (cycleNumber === 0) {
    // First cycle: create the table and insert an initial row
    await db.query(`
      CREATE TABLE IF NOT EXISTS restart_test (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        value TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `)
    await db.query(
      `INSERT INTO restart_test (cycle, value) VALUES ($1, $2)`,
      [cycleNumber, `cycle-${cycleNumber}-init`]
    )
  } else {
    // Subsequent cycles: insert a row with the cycle number
    await db.query(
      `INSERT INTO restart_test (cycle, value) VALUES ($1, $2)`,
      [cycleNumber, `cycle-${cycleNumber}-data`]
    )
  }

  // Signal that our operations completed
  process.send('ready')

  // Do a few more inserts so there's activity to interrupt
  for (let i = 0; i < 10; i++) {
    await db.query(
      `INSERT INTO restart_test (cycle, value) VALUES ($1, $2)`,
      [cycleNumber, `cycle-${cycleNumber}-extra-${i}`]
    )
  }

  process.send('cycle-done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
