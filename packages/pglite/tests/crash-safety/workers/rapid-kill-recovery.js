/**
 * Worker: Kill during WAL recovery
 *
 * Opens PGlite on a dirty data directory (one that was previously SIGKILLed).
 * The parent will SIGKILL this DURING the recovery phase (before waitReady resolves).
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   DO_HEAVY_WRITE  - if "1", do heavy writes if we get past recovery
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const doHeavyWrite = process.env.DO_HEAVY_WRITE === '1'

async function run() {
  process.send('opening')

  const db = new PGlite(dataDir)
  process.send('constructor-done')

  await db.waitReady
  process.send('ready')

  if (doHeavyWrite) {
    // Generate WAL to make the next recovery harder
    await db.query(`
      CREATE TABLE IF NOT EXISTS recovery_stress (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `)

    for (let i = 0; i < 100; i++) {
      await db.query(
        `INSERT INTO recovery_stress (cycle, payload) VALUES ($1, $2)`,
        [Date.now(), 'R'.repeat(800)]
      )
    }
    process.send('heavy-writes-done')
  }

  // Stay alive for parent to kill
  setInterval(() => {}, 60000)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Recovery worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
