/**
 * Worker: Kill during init / waitReady
 *
 * Opens PGlite and optionally does a tiny write.
 * The parent will SIGKILL this before or during waitReady.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   DO_WRITE        - if "1", do a small write after ready
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const doWrite = process.env.DO_WRITE === '1'

async function run() {
  const db = new PGlite(dataDir)

  // Signal that constructor returned (but waitReady not yet resolved)
  process.send('constructor-done')

  await db.waitReady
  process.send('ready')

  if (doWrite) {
    await db.query(`
      CREATE TABLE IF NOT EXISTS init_test (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `)
    process.send('schema-created')

    await db.query(
      `INSERT INTO init_test (cycle, payload) VALUES ($1, $2)`,
      [Date.now(), 'X'.repeat(500)]
    )
    process.send('write-done')
  }

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Init worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
