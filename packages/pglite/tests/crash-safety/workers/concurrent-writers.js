/**
 * Worker: Concurrent writers on the same data directory
 *
 * Two instances of this worker are spawned simultaneously on the same dataDir.
 * WRITER_ID env var ('writer-1' or 'writer-2') identifies the writer.
 *
 * Each worker opens PGlite on the shared dataDir, creates the table if needed,
 * and inserts rows in a tight loop with ~1KB of data each. This reproduces the
 * most reported corruption pattern (Issues #85, #709, #323) where two separate
 * processes write heavily to the same database directory.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const writerId = process.env.WRITER_ID || 'writer-unknown'

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('ready')

  // Ensure table exists (both writers may race on this)
  await db.query(`
    CREATE TABLE IF NOT EXISTS concurrent_test (
      id SERIAL PRIMARY KEY,
      writer TEXT NOT NULL,
      data TEXT NOT NULL,
      seq INT NOT NULL
    )
  `)

  process.send('table_ready')

  // Insert rows in a tight loop
  const padding = 'D'.repeat(1000) // ~1KB of data per row
  for (let seq = 0; seq < 500; seq++) {
    await db.query(
      `INSERT INTO concurrent_test (writer, data, seq) VALUES ($1, $2, $3)`,
      [writerId, `${writerId}-seq${seq}-${padding}`, seq],
    )
    if (seq > 0 && seq % 50 === 0) {
      process.send(`writing:${seq}`)
    }
  }

  process.send('done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error(`Worker ${writerId} error:`, err)
  try {
    process.send(`${writerId}-fatal: ${err.message}`)
  } catch (_) {
    // IPC channel may already be closed
  }
  process.exit(1)
})
