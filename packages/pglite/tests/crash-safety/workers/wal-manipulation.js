/**
 * Worker: WAL manipulation setup
 *
 * Creates a table, inserts data to generate WAL activity, and signals
 * the parent process. The parent will SIGKILL this process and then
 * directly manipulate WAL files, pg_control, and data files.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create a table with an index
  await db.query(`
    CREATE TABLE IF NOT EXISTS wal_test (
      id SERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      checksum TEXT NOT NULL
    )
  `)

  // Create an additional index to make data files more interesting
  await db.query(`CREATE INDEX IF NOT EXISTS idx_wal_test_value ON wal_test (value)`)

  process.send('ready')

  // Insert rows in batches to generate substantial WAL activity
  const batchSize = 50
  const totalBatches = 10
  for (let batch = 0; batch < totalBatches; batch++) {
    for (let i = 0; i < batchSize; i++) {
      const rowNum = batch * batchSize + i
      const value = `row-${rowNum}-${'x'.repeat(200)}`
      const checksum = `chk-${rowNum}`
      await db.query(
        `INSERT INTO wal_test (value, checksum) VALUES ($1, $2)`,
        [value, checksum]
      )
    }
    process.send(`batch-${batch}`)
  }

  process.send('all-inserted')

  // Keep inserting to keep WAL active (parent will kill during this)
  let extra = 0
  while (true) {
    await db.query(
      `INSERT INTO wal_test (value, checksum) VALUES ($1, $2)`,
      [`extra-${extra}-${'z'.repeat(500)}`, `extra-chk-${extra}`]
    )
    extra++
    if (extra % 10 === 0) {
      process.send('inserting')
    }
  }
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
