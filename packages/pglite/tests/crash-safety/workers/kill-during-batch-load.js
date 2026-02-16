/**
 * Worker: Kill during large batch INSERT
 *
 * Creates a table, inserts a baseline batch of 10 rows and syncs,
 * then attempts a huge single INSERT with 5000+ VALUES rows.
 * The parent process will SIGKILL this process while the batch is in flight.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create the test table
  await db.query(`
    CREATE TABLE IF NOT EXISTS batch_test (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      data TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)

  // Insert baseline batch of 10 rows
  const baselineValues = []
  const baselineParams = []
  for (let i = 0; i < 10; i++) {
    const offset = i * 3
    baselineValues.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`)
    baselineParams.push(
      `baseline-${i}`,
      `baseline-data-${i}-${'B'.repeat(80)}`,
      new Date().toISOString()
    )
  }
  await db.query(
    `INSERT INTO batch_test (name, data, created_at) VALUES ${baselineValues.join(', ')}`,
    baselineParams
  )

  // Sync to ensure baseline is durable
  await db.query('SELECT pg_catalog.pg_current_wal_flush_lsn()')

  // Signal that we are about to start the large batch
  process.send('loading')

  // Build a huge INSERT with 5000 VALUES rows
  const batchSize = 5000
  const values = []
  const params = []
  for (let i = 0; i < batchSize; i++) {
    const offset = i * 3
    values.push(`($${offset + 1}, $${offset + 2}, $${offset + 3})`)
    params.push(
      `batch-${i}`,
      `batch-data-${i}-${'X'.repeat(80)}`,
      new Date().toISOString()
    )
  }
  await db.query(
    `INSERT INTO batch_test (name, data, created_at) VALUES ${values.join(', ')}`,
    params
  )

  process.send('done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
