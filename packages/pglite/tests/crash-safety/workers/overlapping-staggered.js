/**
 * Worker: Staggered overlap instances
 *
 * Instance A opens and writes, then instance B opens 500ms later while A is
 * still running. Both write concurrently. This simulates the timing window
 * where a new process starts before the old one fully shuts down.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE           - which cycle this is (0-based)
 *   STAGGER_MS      - delay before opening instance B (default 500)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CYCLE || '0', 10)
const staggerMs = parseInt(process.env.STAGGER_MS || '500', 10)

async function run() {
  const pad = 'S'.repeat(300)

  // Open instance A
  const a = new PGlite(dataDir)
  await a.waitReady
  process.send('a-ready')

  if (cycle === 0) {
    await a.query(`
      CREATE TABLE IF NOT EXISTS stagger_data (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        instance TEXT NOT NULL,
        seq INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `)
    await a.query(`CREATE INDEX IF NOT EXISTS idx_stagger_cycle ON stagger_data (cycle)`)
    process.send('schema-created')
  }

  // Instance A starts a burst of writes
  const aWritePromise = (async () => {
    for (let i = 0; i < 30; i++) {
      await a.query(
        `INSERT INTO stagger_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
        [cycle, 'A', i, `c${cycle}-A-${i}-${pad}`]
      )
      // Vary timing: some fast, some with micro-delays
      if (i % 5 === 0) {
        await new Promise(r => setTimeout(r, 10))
      }
    }
    // Also do some updates to dirty cached pages
    await a.query(
      `UPDATE stagger_data SET payload = payload || '-a-updated' WHERE instance = 'A' AND cycle = $1 AND seq < 10`,
      [cycle]
    )
  })()

  // After stagger delay, open instance B while A is still writing
  await new Promise(r => setTimeout(r, staggerMs))

  const b = new PGlite(dataDir)
  await b.waitReady
  process.send('b-ready')

  // Instance B starts writing immediately (A is likely still writing)
  const bWritePromise = (async () => {
    for (let i = 0; i < 30; i++) {
      await b.query(
        `INSERT INTO stagger_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
        [cycle, 'B', i, `c${cycle}-B-${i}-${pad}`]
      )
    }
    // B also does deletes (touching pages A may have cached)
    try {
      await b.query(
        `DELETE FROM stagger_data WHERE instance = 'A' AND cycle = $1 AND seq > 20`,
        [cycle]
      )
    } catch (_) {}
  })()

  // Wait for both to finish their write bursts
  await Promise.allSettled([aWritePromise, bWritePromise])
  process.send('overlap-writes-done')

  // Now A does more writes with its stale cache
  try {
    for (let i = 50; i < 60; i++) {
      await a.query(
        `INSERT INTO stagger_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
        [cycle, 'A', i, `c${cycle}-A-stale-${i}-${pad}`]
      )
    }
    process.send('a-stale-writes-done')
  } catch (err) {
    process.send(`a-stale-error:${err.message}`)
  }

  process.send('all-done')
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Staggered worker cycle ${cycle} error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
