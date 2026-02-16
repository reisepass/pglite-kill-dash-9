/**
 * Worker: Dev-server restart cycle
 *
 * Simulates a developer using PGlite in a Node.js dev server that gets
 * restarted repeatedly. Each cycle performs different operations depending
 * on the OPERATION env var.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE           - which cycle this is (0-based)
 *   OPERATION       - one of: 'schema', 'insert', 'query', 'migrate'
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CYCLE, 10)
const operation = process.env.OPERATION || 'schema'

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady
  process.send('opened')

  switch (operation) {
    case 'schema':
      await doSchema(db)
      break
    case 'insert':
      await doInsert(db)
      break
    case 'query':
      await doQuery(db)
      break
    case 'migrate':
      await doMigrate(db)
      break
    default:
      throw new Error(`Unknown operation: ${operation}`)
  }

  process.send('operation-done')
  await db.close()
  process.exit(0)
}

async function doSchema(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS dev_items (
      id SERIAL PRIMARY KEY,
      cycle INTEGER NOT NULL,
      name TEXT NOT NULL,
      value DOUBLE PRECISION DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_dev_items_cycle ON dev_items (cycle)
  `)
  await db.query(`
    CREATE TABLE IF NOT EXISTS dev_logs (
      id SERIAL PRIMARY KEY,
      cycle INTEGER NOT NULL,
      message TEXT NOT NULL,
      level TEXT DEFAULT 'info',
      logged_at TIMESTAMP DEFAULT NOW()
    )
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_dev_logs_cycle ON dev_logs (cycle)
  `)
  process.send('schema-created')
  // Seed a few rows
  await db.query(
    `INSERT INTO dev_items (cycle, name, value) VALUES ($1, $2, $3)`,
    [cycle, `schema-init-${cycle}`, Math.random() * 100]
  )
  await db.query(
    `INSERT INTO dev_logs (cycle, message) VALUES ($1, $2)`,
    [cycle, `Schema setup completed for cycle ${cycle}`]
  )
}

async function doInsert(db) {
  // Insert 20 rows with varied data into dev_items
  for (let i = 0; i < 20; i++) {
    await db.query(
      `INSERT INTO dev_items (cycle, name, value) VALUES ($1, $2, $3)`,
      [cycle, `item-${cycle}-${i}`, Math.random() * 1000]
    )
  }
  // Also insert some log entries
  for (let i = 0; i < 5; i++) {
    await db.query(
      `INSERT INTO dev_logs (cycle, message, level) VALUES ($1, $2, $3)`,
      [cycle, `Insert batch ${i} for cycle ${cycle}`, i % 2 === 0 ? 'info' : 'debug']
    )
  }
}

async function doQuery(db) {
  // Run various read queries - aggregates, filters, subqueries
  await db.query(`SELECT count(*) FROM dev_items`)
  await db.query(`SELECT cycle, count(*) as cnt FROM dev_items GROUP BY cycle ORDER BY cycle`)
  await db.query(`SELECT avg(value), min(value), max(value) FROM dev_items`)
  await db.query(`
    SELECT i.cycle, i.name, i.value, l.message
    FROM dev_items i
    LEFT JOIN dev_logs l ON i.cycle = l.cycle
    ORDER BY i.id DESC
    LIMIT 50
  `)
  await db.query(`
    SELECT cycle, count(*) as item_count
    FROM dev_items
    WHERE value > 500
    GROUP BY cycle
    HAVING count(*) > 0
    ORDER BY item_count DESC
  `)
  // Insert a log that we queried
  await db.query(
    `INSERT INTO dev_logs (cycle, message) VALUES ($1, $2)`,
    [cycle, `Query cycle ${cycle} completed`]
  )
}

async function doMigrate(db) {
  // Simulate incremental migrations - using IF NOT EXISTS / IF EXISTS patterns
  // so they are idempotent across cycles
  await db.query(`
    ALTER TABLE dev_items ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'general'
  `)
  await db.query(`
    ALTER TABLE dev_items ADD COLUMN IF NOT EXISTS priority INTEGER DEFAULT 0
  `)
  await db.query(`
    ALTER TABLE dev_logs ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'app'
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_dev_items_category ON dev_items (category)
  `)
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_dev_items_priority ON dev_items (priority)
  `)
  // Insert a migration log
  await db.query(
    `INSERT INTO dev_logs (cycle, message, level) VALUES ($1, $2, $3)`,
    [cycle, `Migration completed for cycle ${cycle}`, 'info']
  )
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
