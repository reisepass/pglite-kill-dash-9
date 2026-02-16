/**
 * Worker: WAL truncation corruption test
 *
 * Creates tables and does heavy writes to generate significant WAL data.
 * Does NOT checkpoint, ensuring the WAL contains all the data needed for
 * recovery. The parent process will SIGKILL this worker, then tamper with
 * WAL files to simulate partial/truncated writes (power loss scenario).
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

function largePayload(i, sizeBytes) {
  const base = `payload-${i}-` + 'X'.repeat(100)
  return base.repeat(Math.ceil(sizeBytes / base.length)).slice(0, sizeBytes)
}

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('ready')

  // Create multiple tables with indexes to maximize WAL complexity
  await db.query(`
    CREATE TABLE IF NOT EXISTS wal_test_main (
      id SERIAL PRIMARY KEY,
      data TEXT NOT NULL,
      checksum TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await db.query(`
    CREATE TABLE IF NOT EXISTS wal_test_secondary (
      id SERIAL PRIMARY KEY,
      ref_id INTEGER REFERENCES wal_test_main(id),
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      body TEXT
    )
  `)
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_main_checksum ON wal_test_main(checksum)`
  )
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_secondary_ref ON wal_test_secondary(ref_id)`
  )
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_secondary_meta ON wal_test_secondary USING GIN (metadata)`
  )

  process.send('schema-created')

  // Insert rows into main table (5KB payload each, 100 rows = ~500KB WAL)
  for (let i = 0; i < 100; i++) {
    const payload = largePayload(i, 5000)
    await db.query(
      `INSERT INTO wal_test_main (data, checksum) VALUES ($1, $2)`,
      [payload, `chk-${i}-${payload.length}`]
    )
  }

  process.send('main-inserted')

  // Insert rows into secondary table with JSONB data
  for (let i = 0; i < 100; i++) {
    await db.query(
      `INSERT INTO wal_test_secondary (ref_id, metadata, body) VALUES ($1, $2, $3)`,
      [
        i + 1,
        JSON.stringify({ batch: 1, index: i, tags: ['wal-test', `item-${i}`] }),
        largePayload(i + 1000, 3000),
      ]
    )
  }

  process.send('secondary-inserted')

  // UPDATE all main rows - doubles WAL since old+new versions exist
  await db.query(
    `UPDATE wal_test_main SET data = data || $1, checksum = 'updated-' || checksum`,
    [' UPDATED-ROUND-1']
  )

  process.send('main-updated')

  // Do a large transaction to create more WAL entries
  await db.query('BEGIN')
  for (let i = 100; i < 200; i++) {
    await db.query(
      `INSERT INTO wal_test_main (data, checksum) VALUES ($1, $2)`,
      [largePayload(i, 5000), `chk-${i}`]
    )
  }
  await db.query('COMMIT')

  process.send('batch-committed')

  // Continue writing without checkpointing - keep generating WAL
  for (let i = 0; i < 50; i++) {
    await db.query(
      `UPDATE wal_test_secondary SET metadata = metadata || $1 WHERE ref_id = $2`,
      [JSON.stringify({ updated: true, round: i }), (i % 100) + 1]
    )
    if (i % 10 === 0) {
      process.send('still-writing')
    }
  }

  process.send('writes-complete')

  // Intentionally NO checkpoint
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
