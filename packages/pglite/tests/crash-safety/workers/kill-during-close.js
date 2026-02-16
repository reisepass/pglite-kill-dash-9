import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create table with substantial data to make WAL large
  await db.query(`
    CREATE TABLE IF NOT EXISTS close_test (
      id SERIAL PRIMARY KEY,
      value TEXT NOT NULL,
      padding TEXT NOT NULL
    )
  `)

  // Insert 500+ rows with ~5KB text each to generate a large WAL
  const padding = 'x'.repeat(5000)
  for (let batch = 0; batch < 10; batch++) {
    const values = []
    const params = []
    for (let i = 0; i < 50; i++) {
      const idx = batch * 50 + i
      const paramBase = i * 2
      values.push(`($${paramBase + 1}, $${paramBase + 2})`)
      params.push(`row-${idx}`, padding)
    }
    await db.query(
      `INSERT INTO close_test (value, padding) VALUES ${values.join(', ')}`,
      params
    )
  }

  // Do UPDATEs to create dirty pages
  await db.query(
    `UPDATE close_test SET value = 'updated-' || id WHERE id % 3 = 0`
  )

  // Create an index to add complexity
  await db.query(`CREATE INDEX idx_close_test_value ON close_test (value)`)

  // Signal that all data operations are done
  process.send('loaded')

  // Signal right before calling close — the test will kill us here
  process.send('closing')

  // close() triggers _pgl_shutdown() which performs WAL checkpoint
  await db.close()

  // May never arrive if killed during checkpoint
  process.send('closed')
}

run().catch((err) => {
  process.send(`error: ${err.message}`)
  process.exit(1)
})
