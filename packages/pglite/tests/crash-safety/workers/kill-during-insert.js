/**
 * Worker: Kill during bulk INSERT
 *
 * Creates a table and inserts many rows in a loop (not in a transaction).
 * The parent process will SIGKILL this process while inserts are in progress.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create the test table
  await db.query(`
    CREATE TABLE IF NOT EXISTS crash_test (
      id SERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)

  // Signal that table is created and we're about to start inserting
  process.send('ready')

  // Insert many rows one at a time (not in a transaction)
  const totalRows = 500
  for (let i = 0; i < totalRows; i++) {
    await db.query(
      `INSERT INTO crash_test (value) VALUES ($1)`,
      [`row-${i}-${'x'.repeat(200)}`]
    )

    // Signal periodically so the parent can kill us mid-insert
    if (i % 5 === 0) {
      process.send('inserting')
    }
  }

  process.send('done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
