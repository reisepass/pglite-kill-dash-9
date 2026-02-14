/**
 * Worker: Kill during WAL recovery (double-crash)
 *
 * Accepts a PHASE env var (1, 2, or 3) to implement a 3-phase crash scenario:
 *   Phase 1: Create table and insert 500 rows with 5KB text blobs, then stay
 *            alive (parent will SIGKILL while inserts are in progress).
 *   Phase 2: Open PGlite on the dirty datadir (triggers WAL recovery).
 *            Parent will SIGKILL during recovery startup.
 *   Phase 3: Open PGlite, query data, send results back.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   PHASE           - which phase to run (1, 2, or 3)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const phase = parseInt(process.env.PHASE, 10)

async function phase1() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create the test table with a large text column
  await db.query(`
    CREATE TABLE IF NOT EXISTS recovery_test (
      id SERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)

  process.send('ready')

  // Insert 500 rows with ~5KB text each (not in a transaction, so some commit)
  const blob = 'A'.repeat(5000)
  const totalRows = 500
  for (let i = 0; i < totalRows; i++) {
    await db.query(
      `INSERT INTO recovery_test (value) VALUES ($1)`,
      [`row-${i}-${blob}`]
    )

    if (i % 5 === 0) {
      process.send('inserting')
    }
  }

  process.send('done')
  await db.close()
  process.exit(0)
}

async function phase2() {
  // Signal immediately that we are about to open (recovery will happen here)
  process.send('opening')

  const db = new PGlite(dataDir)
  await db.waitReady

  // If we get here, recovery finished before we were killed
  process.send('recovered')
  await db.close()
  process.exit(0)
}

async function phase3() {
  process.send('opening')

  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('opened')

  // Check if our table exists
  const tables = await db.query(`
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public' AND tablename = 'recovery_test'
  `)

  if (tables.rows.length === 0) {
    process.send(JSON.stringify({ tableExists: false, rowCount: 0 }))
  } else {
    const countResult = await db.query(
      'SELECT count(*) as cnt FROM recovery_test'
    )
    const rowCount = parseInt(countResult.rows[0].cnt, 10)
    process.send(JSON.stringify({ tableExists: true, rowCount }))
  }

  await db.close()
  process.exit(0)
}

async function run() {
  switch (phase) {
    case 1:
      return phase1()
    case 2:
      return phase2()
    case 3:
      return phase3()
    default:
      console.error(`Unknown phase: ${phase}`)
      process.exit(1)
  }
}

run().catch((err) => {
  console.error(`Worker error (phase ${phase}):`, err)
  process.exit(1)
})
