/**
 * Worker: DDL collision - one instance does DDL while another does DML
 *
 * Instance A creates tables and indexes while instance B does inserts and
 * updates. DDL operations modify system catalog pages, which are particularly
 * dangerous when two instances have stale views of each other's changes.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE           - which cycle this is (0-based)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CYCLE || '0', 10)

async function run() {
  const pad = 'D'.repeat(200)

  // Open instance A (the DDL instance)
  const a = new PGlite(dataDir)
  await a.waitReady
  process.send('a-ready')

  // Base table always exists
  if (cycle === 0) {
    await a.query(`
      CREATE TABLE IF NOT EXISTS ddl_base (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        data TEXT NOT NULL
      )
    `)
    process.send('base-schema-created')
  }

  // Seed some data for this cycle
  for (let i = 0; i < 10; i++) {
    await a.query(
      `INSERT INTO ddl_base (cycle, data) VALUES ($1, $2)`,
      [cycle, `seed-${i}-${pad}`]
    )
  }
  process.send('a-seeded')

  // Open instance B WITHOUT closing A
  const b = new PGlite(dataDir)
  await b.waitReady
  process.send('b-ready')

  // Now run DDL on A and DML on B concurrently
  const ddlWork = (async () => {
    // Create a new table each cycle
    const tbl = `ddl_table_c${cycle}`
    try {
      await a.query(`
        CREATE TABLE ${tbl} (
          id SERIAL PRIMARY KEY,
          val TEXT NOT NULL,
          num INTEGER DEFAULT 0
        )
      `)
      process.send('ddl-table-created')

      // Add columns
      await a.query(`ALTER TABLE ${tbl} ADD COLUMN extra TEXT DEFAULT 'none'`)
      process.send('ddl-alter-done')

      // Create indexes
      await a.query(`CREATE INDEX idx_${tbl}_val ON ${tbl} (val)`)
      await a.query(`CREATE INDEX idx_${tbl}_num ON ${tbl} (num)`)
      process.send('ddl-indexes-created')

      // Insert into the new table
      for (let i = 0; i < 20; i++) {
        await a.query(
          `INSERT INTO ${tbl} (val, num, extra) VALUES ($1, $2, $3)`,
          [`val-${i}`, i * 10, `extra-${i}-${pad}`]
        )
      }
      process.send('ddl-inserts-done')
    } catch (err) {
      process.send(`ddl-error:${err.message}`)
    }
  })()

  const dmlWork = (async () => {
    try {
      // B does heavy inserts into the base table while A does DDL
      for (let i = 0; i < 40; i++) {
        await b.query(
          `INSERT INTO ddl_base (cycle, data) VALUES ($1, $2)`,
          [cycle, `b-insert-${i}-${pad}`]
        )
      }
      process.send('dml-inserts-done')

      // B does updates (dirtying pages that A's catalog changes may conflict with)
      await b.query(
        `UPDATE ddl_base SET data = data || '-updated' WHERE cycle = $1`,
        [cycle]
      )
      process.send('dml-updates-done')

      // B does deletes
      await b.query(
        `DELETE FROM ddl_base WHERE cycle = $1 AND id % 3 = 0`,
        [cycle]
      )
      process.send('dml-deletes-done')
    } catch (err) {
      process.send(`dml-error:${err.message}`)
    }
  })()

  await Promise.allSettled([ddlWork, dmlWork])
  process.send('all-done')

  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`DDL collision worker cycle ${cycle} error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
