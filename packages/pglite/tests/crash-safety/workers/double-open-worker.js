/**
 * Worker: Concurrent double-open on the same data directory
 *
 * Two instances of this worker are spawned simultaneously on the same dataDir.
 * INSTANCE_ID ('A' or 'B') determines behavior:
 *   - Instance A: Creates a table and inserts rows in a loop
 *   - Instance B: Waits briefly, then tries to SELECT and INSERT
 *
 * This tests that PGlite should lock the data directory to prevent
 * concurrent access, which currently causes corruption (issue #85).
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const instanceId = process.env.INSTANCE_ID

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('running')

  if (instanceId === 'A') {
    // Instance A: create table and insert rows continuously
    await db.query(`
      CREATE TABLE IF NOT EXISTS double_open_test (
        id SERIAL PRIMARY KEY,
        instance TEXT NOT NULL,
        value TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `)

    process.send('table_created')

    for (let i = 0; i < 500; i++) {
      await db.query(
        `INSERT INTO double_open_test (instance, value) VALUES ($1, $2)`,
        ['A', `row-A-${i}-${'x'.repeat(100)}`]
      )
      if (i % 10 === 0) {
        process.send(`A-inserting-${i}`)
      }
    }

    process.send('A-done')
  } else if (instanceId === 'B') {
    // Instance B: wait briefly then try to read and write
    await new Promise((resolve) => setTimeout(resolve, 200))

    try {
      // Try to read
      const result = await db.query(`
        SELECT count(*) as cnt FROM double_open_test
      `)
      process.send(`B-read-count-${result.rows[0].cnt}`)
    } catch (err) {
      process.send(`B-read-error: ${err.message}`)
    }

    // Try to insert rows
    for (let i = 0; i < 200; i++) {
      try {
        await db.query(
          `INSERT INTO double_open_test (instance, value) VALUES ($1, $2)`,
          ['B', `row-B-${i}-${'y'.repeat(100)}`]
        )
      } catch (err) {
        process.send(`B-insert-error: ${err.message}`)
        break
      }
      if (i % 10 === 0) {
        process.send(`B-inserting-${i}`)
      }
    }

    process.send('B-done')
  }

  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error(`Worker ${instanceId} error:`, err)
  process.send(`${instanceId}-fatal: ${err.message}`)
  process.exit(1)
})
