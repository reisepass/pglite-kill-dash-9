/**
 * Worker: DDL or DML writer (configurable via env)
 *
 * Runs either DDL operations (CREATE TABLE, ALTER TABLE, CREATE INDEX)
 * or DML operations (INSERT, UPDATE, DELETE) based on WRITER_MODE env var.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   WRITER_MODE     - "ddl" or "dml"
 *   CYCLE           - which cycle this is (0-based)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const mode = process.env.WRITER_MODE || 'dml'
const cycle = parseInt(process.env.CYCLE || '0', 10)

async function run() {
  const pad = 'D'.repeat(300)

  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  if (mode === 'ddl') {
    // DDL mode: create tables, indexes, alter tables
    try {
      // Ensure base table exists
      await db.query(`
        CREATE TABLE IF NOT EXISTS ddl_base (
          id SERIAL PRIMARY KEY,
          cycle INTEGER NOT NULL,
          data TEXT NOT NULL
        )
      `)
      process.send('base-table-ready')

      // Create a new table for this cycle
      const tbl = `ddl_cycle_${cycle}`
      await db.query(`
        CREATE TABLE IF NOT EXISTS ${tbl} (
          id SERIAL PRIMARY KEY,
          val TEXT NOT NULL,
          num INTEGER DEFAULT 0
        )
      `)
      process.send('new-table-created')

      // Alter the new table
      await db.query(`ALTER TABLE ${tbl} ADD COLUMN IF NOT EXISTS extra TEXT DEFAULT 'none'`)
      process.send('alter-done')

      // Create indexes
      await db.query(`CREATE INDEX IF NOT EXISTS idx_${tbl}_val ON ${tbl} (val)`)
      await db.query(`CREATE INDEX IF NOT EXISTS idx_${tbl}_num ON ${tbl} (num)`)
      process.send('indexes-created')

      // Insert into the new table
      for (let i = 0; i < 30; i++) {
        await db.query(
          `INSERT INTO ${tbl} (val, num, extra) VALUES ($1, $2, $3)`,
          [`val-${i}`, i * 10, `extra-${i}-${pad}`]
        )
      }
      process.send('ddl-inserts-done')

      // Also insert into base table
      for (let i = 0; i < 20; i++) {
        await db.query(
          `INSERT INTO ddl_base (cycle, data) VALUES ($1, $2)`,
          [cycle, `ddl-writer-${i}-${pad}`]
        )
      }
      process.send('ddl-base-inserts-done')
    } catch (err) {
      process.send(`ddl-error:${err.message}`)
    }
  } else {
    // DML mode: heavy inserts, updates, deletes on base table
    try {
      await db.query(`
        CREATE TABLE IF NOT EXISTS ddl_base (
          id SERIAL PRIMARY KEY,
          cycle INTEGER NOT NULL,
          data TEXT NOT NULL
        )
      `)
      process.send('base-table-ready')

      // Heavy inserts
      for (let i = 0; i < 50; i++) {
        await db.query(
          `INSERT INTO ddl_base (cycle, data) VALUES ($1, $2)`,
          [cycle, `dml-writer-${i}-${pad}`]
        )
      }
      process.send('dml-inserts-done')

      // Updates (dirty pages)
      await db.query(
        `UPDATE ddl_base SET data = data || '-updated' WHERE cycle = $1`,
        [cycle]
      )
      process.send('dml-updates-done')

      // Deletes
      await db.query(
        `DELETE FROM ddl_base WHERE cycle = $1 AND id % 3 = 0`,
        [cycle]
      )
      process.send('dml-deletes-done')
    } catch (err) {
      process.send(`dml-error:${err.message}`)
    }
  }

  process.send('all-done')

  // Keep alive for SIGKILL
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`DDL writer (${mode}) cycle ${cycle} error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
