/**
 * Worker: Massive WAL then kill (Issue #819 WAL bloat)
 *
 * Creates a table with TEXT columns and a generated tsvector column with a GIN
 * index. Inserts 200 large rows (10KB each), updates them all, inserts 200
 * more, and updates again — all without CHECKPOINT or VACUUM so the WAL grows
 * very large. The parent process will SIGKILL during or after these operations.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

function largeText(seed, sizeBytes) {
  const base = `row-${seed}-` + 'A'.repeat(200)
  return base.repeat(Math.ceil(sizeBytes / base.length)).slice(0, sizeBytes)
}

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('ready')

  // Create table with generated tsvector column and GIN index
  await db.query(`
    CREATE TABLE IF NOT EXISTS docs (
      id SERIAL PRIMARY KEY,
      title TEXT,
      body TEXT,
      body_tsv TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || body)) STORED
    )
  `)
  await db.query(
    `CREATE INDEX IF NOT EXISTS docs_tsv_idx ON docs USING GIN (body_tsv)`
  )

  process.send('schema-created')

  // Insert first batch of 200 rows (~10KB body each)
  for (let i = 0; i < 200; i++) {
    await db.query(
      `INSERT INTO docs (title, body) VALUES ($1, $2)`,
      [`title-${i}`, largeText(i, 10000)]
    )
  }

  process.send('insert-1-done')

  // Update all rows — doubles WAL since old + new versions exist
  await db.query(
    `UPDATE docs SET body = body || $1 WHERE id <= 200`,
    [' UPDATED-PASS-1']
  )

  process.send('update-1-done')

  // Insert second batch of 200 rows
  for (let i = 200; i < 400; i++) {
    await db.query(
      `INSERT INTO docs (title, body) VALUES ($1, $2)`,
      [`title-${i}`, largeText(i, 10000)]
    )
  }

  process.send('insert-2-done')

  // Update all rows again — WAL grows even more
  await db.query(
    `UPDATE docs SET body = body || $1`,
    [' UPDATED-PASS-2']
  )

  process.send('update-2-done')

  // Intentionally NO checkpoint or vacuum

  process.send('done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
