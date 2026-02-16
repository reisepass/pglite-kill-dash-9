/**
 * Worker: Kill during CHECKPOINT
 *
 * Creates tables and inserts massive amounts of data WITHOUT checkpointing,
 * building up a huge dirty WAL. Then issues CHECKPOINT which must flush
 * all those dirty buffers to data files. The large WAL makes the checkpoint
 * take longer, giving the parent more time to kill during it.
 *
 * Key techniques to maximize checkpoint duration:
 * 1. Insert lots of data across many tables (spreads dirty pages)
 * 2. Create indexes (more pages to flush)
 * 3. Do updates after inserts (creates even more dirty pages)
 * 4. Use large payloads to fill more pages
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CRASH_CYCLE || '0', 10)

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  process.send('ready')

  if (cycle === 0) {
    // Create multiple tables to spread dirty pages across many files
    for (let t = 0; t < 5; t++) {
      await db.query(`
        CREATE TABLE IF NOT EXISTS ckpt_t${t} (
          id SERIAL PRIMARY KEY,
          cycle INT NOT NULL,
          seq INT NOT NULL,
          tag TEXT NOT NULL,
          payload TEXT NOT NULL,
          extra BYTEA
        )
      `)
      await db.query(
        `CREATE INDEX IF NOT EXISTS idx_ckpt_t${t}_tag ON ckpt_t${t}(tag)`
      )
      await db.query(
        `CREATE INDEX IF NOT EXISTS idx_ckpt_t${t}_cycle ON ckpt_t${t}(cycle)`
      )
    }
    await db.query(`
      CREATE TABLE IF NOT EXISTS ckpt_meta (
        id SERIAL PRIMARY KEY,
        cycle INT NOT NULL,
        table_name TEXT NOT NULL,
        row_count INT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `)
  }

  // Insert massive data across all 5 tables
  const rowsPerTable = 1000
  const batchSize = 25
  // ~2KB payload per row, 5 tables * 1000 rows = ~10MB of data in WAL
  const payload = 'D'.repeat(2000)

  for (let t = 0; t < 5; t++) {
    for (let batch = 0; batch < rowsPerTable / batchSize; batch++) {
      await db.query('BEGIN')
      for (let i = 0; i < batchSize; i++) {
        const seq = batch * batchSize + i
        await db.query(
          `INSERT INTO ckpt_t${t} (cycle, seq, tag, payload) VALUES ($1, $2, $3, $4)`,
          [cycle, seq, `tag_${cycle}_${t}_${seq}`, payload]
        )
      }
      await db.query('COMMIT')
    }
  }

  // Do heavy updates to dirty even more pages
  for (let t = 0; t < 5; t++) {
    await db.query(
      `UPDATE ckpt_t${t} SET payload = payload || '_UPD', tag = 'updated_' || tag WHERE cycle = $1`,
      [cycle]
    )
  }

  // Record metadata
  for (let t = 0; t < 5; t++) {
    const cnt = await db.query(
      `SELECT count(*)::int AS cnt FROM ckpt_t${t} WHERE cycle = $1`,
      [cycle]
    )
    await db.query(
      `INSERT INTO ckpt_meta (cycle, table_name, row_count) VALUES ($1, $2, $3)`,
      [cycle, `ckpt_t${t}`, cnt.rows[0].cnt]
    )
  }

  process.send('data-loaded')

  // Critical moment: signal checkpoint start and issue CHECKPOINT
  // The process.send is async IPC, so send it then immediately call CHECKPOINT
  // The parent will kill us while CHECKPOINT is flushing dirty pages
  process.send('checkpoint-starting')
  await db.query('CHECKPOINT')

  process.send('checkpoint-done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  try {
    process.send(`error: ${err.message}`)
  } catch (_) {
    // IPC may already be gone
  }
  process.exit(1)
})
