/**
 * Worker: Schema evolution across kills
 *
 * Each cycle does a different DDL operation:
 *   0: CREATE TABLE
 *   1: ALTER TABLE ADD COLUMN
 *   2: CREATE INDEX
 *   3: INSERT data
 *   4: ALTER TABLE ADD COLUMN (another)
 *   5: CREATE another TABLE
 *   6: DROP TABLE (the second one)
 *   7: CREATE INDEX (partial)
 *   8: ALTER TABLE DROP COLUMN
 *   9: VACUUM (WAL-heavy)
 *   10+: repeat pattern
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE_NUM       - which cycle this is (determines which DDL op to do)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycleNum = parseInt(process.env.CYCLE_NUM || '0', 10)

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('ready')

  const op = cycleNum % 10

  switch (op) {
    case 0: {
      // CREATE TABLE
      await db.query(`
        CREATE TABLE IF NOT EXISTS evolving (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL DEFAULT 'initial',
          value INTEGER DEFAULT 0
        )
      `)
      await db.query(`INSERT INTO evolving (name, value) VALUES ('seed', ${cycleNum})`)
      process.send('ddl:create-table')
      break
    }
    case 1: {
      // ALTER TABLE ADD COLUMN
      const colName = `col_${cycleNum}`
      await db.query(`ALTER TABLE evolving ADD COLUMN IF NOT EXISTS ${colName} TEXT DEFAULT NULL`)
      await db.query(`UPDATE evolving SET ${colName} = 'added-at-cycle-${cycleNum}'`)
      process.send('ddl:add-column')
      break
    }
    case 2: {
      // CREATE INDEX
      await db.query(`CREATE INDEX IF NOT EXISTS idx_evolving_c${cycleNum} ON evolving (value)`)
      // Also insert some data for the index to cover
      for (let i = 0; i < 50; i++) {
        await db.query(`INSERT INTO evolving (name, value) VALUES ($1, $2)`,
          [`idx-cycle-${cycleNum}`, cycleNum * 1000 + i])
      }
      process.send('ddl:create-index')
      break
    }
    case 3: {
      // Heavy INSERT
      for (let i = 0; i < 200; i++) {
        await db.query(`INSERT INTO evolving (name, value) VALUES ($1, $2)`,
          [`data-${cycleNum}-${i}`, i])
      }
      process.send('ddl:heavy-insert')
      break
    }
    case 4: {
      // ALTER TABLE ADD another COLUMN + backfill
      const colName = `extra_${cycleNum}`
      await db.query(`ALTER TABLE evolving ADD COLUMN IF NOT EXISTS ${colName} INTEGER DEFAULT 0`)
      await db.query(`UPDATE evolving SET ${colName} = id * ${cycleNum}`)
      process.send('ddl:add-column-backfill')
      break
    }
    case 5: {
      // CREATE another TABLE + foreign-key-like data
      await db.query(`
        CREATE TABLE IF NOT EXISTS evolving_child_${cycleNum} (
          id SERIAL PRIMARY KEY,
          parent_id INTEGER,
          data TEXT DEFAULT 'child-row'
        )
      `)
      for (let i = 0; i < 30; i++) {
        await db.query(`INSERT INTO evolving_child_${cycleNum} (parent_id, data) VALUES ($1, $2)`,
          [i + 1, `child-${cycleNum}-${i}`])
      }
      process.send('ddl:create-child-table')
      break
    }
    case 6: {
      // DROP TABLE (try to drop a previous child table)
      const targetCycle = cycleNum - 1
      try {
        await db.query(`DROP TABLE IF EXISTS evolving_child_${targetCycle}`)
        process.send('ddl:drop-table')
      } catch (err) {
        process.send(`ddl:drop-table-error:${err.message}`)
      }
      break
    }
    case 7: {
      // CREATE partial INDEX
      await db.query(`
        CREATE INDEX IF NOT EXISTS idx_evolving_partial_${cycleNum}
        ON evolving (name)
        WHERE value > ${cycleNum * 100}
      `)
      process.send('ddl:partial-index')
      break
    }
    case 8: {
      // Try to drop a previously added column
      const targetCol = `col_${cycleNum - 7}` // Should target a col added in case 1
      try {
        await db.query(`ALTER TABLE evolving DROP COLUMN IF EXISTS ${targetCol}`)
        process.send('ddl:drop-column')
      } catch (err) {
        process.send(`ddl:drop-column-error:${err.message}`)
      }
      break
    }
    case 9: {
      // VACUUM + ANALYZE (WAL-heavy)
      await db.query(`VACUUM evolving`)
      await db.query(`ANALYZE evolving`)
      // Also do some writes post-vacuum
      for (let i = 0; i < 20; i++) {
        await db.query(`INSERT INTO evolving (name, value) VALUES ($1, $2)`,
          [`post-vacuum-${cycleNum}`, i])
      }
      process.send('ddl:vacuum-analyze')
      break
    }
  }

  process.send(`cycle-complete:${cycleNum}`)

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Schema evolution worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
