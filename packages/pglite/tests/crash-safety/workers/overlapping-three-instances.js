/**
 * Worker: Three simultaneous PGlite instances on the same data directory
 *
 * Opens THREE PGlite instances on the same data dir without closing any,
 * then all three write concurrently. This is worse than the HMR double-instance
 * case because three separate WASM heaps all have stale views of the same pages.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE           - which cycle this is (0-based)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CYCLE || '0', 10)

async function run() {
  // Open instance A
  const a = new PGlite(dataDir)
  await a.waitReady
  process.send('instance-a-ready')

  // Create schema on first cycle
  if (cycle === 0) {
    await a.query(`
      CREATE TABLE IF NOT EXISTS triple_data (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        instance TEXT NOT NULL,
        seq INTEGER NOT NULL,
        payload TEXT NOT NULL
      )
    `)
    await a.query(`CREATE INDEX IF NOT EXISTS idx_triple_cycle ON triple_data (cycle)`)
    process.send('schema-created')
  }

  // Instance A writes some data
  const pad = 'X'.repeat(400)
  for (let i = 0; i < 10; i++) {
    await a.query(
      `INSERT INTO triple_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
      [cycle, 'A', i, `c${cycle}-A-${i}-${pad}`]
    )
  }
  process.send('a-wrote')

  // Open instance B WITHOUT closing A
  const b = new PGlite(dataDir)
  await b.waitReady
  process.send('instance-b-ready')

  // Instance B writes
  for (let i = 0; i < 10; i++) {
    await b.query(
      `INSERT INTO triple_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
      [cycle, 'B', i, `c${cycle}-B-${i}-${pad}`]
    )
  }
  process.send('b-wrote')

  // Open instance C WITHOUT closing A or B
  const c = new PGlite(dataDir)
  await c.waitReady
  process.send('instance-c-ready')

  // Now all three write concurrently using Promise.all
  const writeAll = async (inst, name, start, count) => {
    for (let i = start; i < start + count; i++) {
      await inst.query(
        `INSERT INTO triple_data (cycle, instance, seq, payload) VALUES ($1, $2, $3, $4)`,
        [cycle, name, i, `c${cycle}-${name}-concurrent-${i}-${pad}`]
      )
    }
  }

  await Promise.all([
    writeAll(a, 'A', 100, 20),
    writeAll(b, 'B', 100, 20),
    writeAll(c, 'C', 100, 20),
  ])
  process.send('concurrent-writes-done')

  // A does updates while B and C do inserts (more page conflict)
  try {
    await Promise.all([
      a.query(`UPDATE triple_data SET payload = 'A-updated' WHERE instance = 'A' AND cycle = $1 AND seq < 5`, [cycle]),
      b.query(`INSERT INTO triple_data (cycle, instance, seq, payload) VALUES ($1, 'B', 200, 'b-extra')`, [cycle]),
      c.query(`INSERT INTO triple_data (cycle, instance, seq, payload) VALUES ($1, 'C', 200, 'c-extra')`, [cycle]),
    ])
  } catch (err) {
    process.send(`mixed-ops-error:${err.message}`)
  }

  process.send('all-done')

  // Keep alive for SIGKILL - no close() on any instance
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Three-instance worker cycle ${cycle} error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
