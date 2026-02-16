import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create a table and insert many rows
  await db.query(`
    CREATE TABLE IF NOT EXISTS vacuum_test (
      id SERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      padding TEXT NOT NULL
    )
  `)

  // Insert 500 rows to give VACUUM plenty of work
  for (let i = 0; i < 500; i++) {
    await db.query(
      `INSERT INTO vacuum_test (value, padding) VALUES ($1, $2)`,
      [`row-${i}`, 'x'.repeat(200)]
    )
  }

  // Delete roughly half the rows to create dead tuples for VACUUM to clean
  await db.query(`DELETE FROM vacuum_test WHERE id % 2 = 0`)

  // Signal that we're about to vacuum, then start VACUUM FULL
  process.send('vacuuming')

  await db.query('VACUUM FULL vacuum_test')

  // If we get here, vacuum completed before kill
  process.send('vacuum_done')
  await db.close()
}

run().catch((err) => {
  process.send(`error: ${err.message}`)
  process.exit(1)
})
