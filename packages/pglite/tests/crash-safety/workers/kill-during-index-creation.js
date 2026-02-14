import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create a table and insert many rows so index creation takes time
  await db.query(`
    CREATE TABLE IF NOT EXISTS indexed_data (
      id SERIAL PRIMARY KEY,
      value INTEGER NOT NULL,
      payload TEXT NOT NULL
    )
  `)

  // Insert 1500 rows in batches to build up enough data
  for (let batch = 0; batch < 15; batch++) {
    const values = []
    for (let i = 0; i < 100; i++) {
      const id = batch * 100 + i
      values.push(`(${id}, '${`payload-data-string-${id}-`.repeat(10)}')`)
    }
    await db.query(
      `INSERT INTO indexed_data (value, payload) VALUES ${values.join(', ')}`
    )
  }

  process.send('data-inserted')

  // Signal right before index creation
  process.send('creating-index')

  // Create an index -- this is the operation we want to kill mid-way
  await db.query(
    `CREATE INDEX idx_indexed_data_value ON indexed_data (value)`
  )

  process.send('index-done')

  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
