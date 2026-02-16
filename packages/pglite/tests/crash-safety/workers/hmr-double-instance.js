/**
 * Worker: HMR (Hot Module Reload) double-instance simulation
 *
 * Simulates what happens during HMR in dev servers like Vite/Next.js:
 *   1. Opens PGlite instance A on the data dir, does operations
 *   2. WITHOUT closing instance A, attempts to open PGlite instance B
 *      on the SAME data dir — this should now FAIL due to file locking
 *   3. If the lock prevents instance B, that's the correct behavior
 *   4. The process is killed externally (simulating dev server restart)
 *
 * With the file lock implementation, instance B should fail to open,
 * preventing the corruption that previously occurred from overlapping
 * instances with stale WASM heaps.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR - path to the data directory
 *   CYCLE           - which cycle this is (0-based)
 *   OVERLAP_OPS     - number of operations to do while both instances exist
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const cycle = parseInt(process.env.CYCLE || '0', 10)
const overlapOps = parseInt(process.env.OVERLAP_OPS || '20', 10)

async function run() {
  // ---- Phase 1: Open instance A (the "old module") ----
  const instanceA = new PGlite(dataDir)
  await instanceA.waitReady
  process.send('instance-a-ready')

  // Instance A creates schema on first cycle, then does writes
  if (cycle === 0) {
    await instanceA.query(`
      CREATE TABLE IF NOT EXISTS hmr_data (
        id SERIAL PRIMARY KEY,
        cycle INTEGER NOT NULL,
        instance TEXT NOT NULL,
        phase TEXT NOT NULL,
        seq INTEGER NOT NULL,
        payload TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      )
    `)
    await instanceA.query(`
      CREATE INDEX IF NOT EXISTS idx_hmr_data_cycle ON hmr_data (cycle)
    `)
    await instanceA.query(`
      CREATE INDEX IF NOT EXISTS idx_hmr_data_instance ON hmr_data (instance)
    `)
    process.send('schema-created')
  }

  // Instance A does writes
  const padding = 'A'.repeat(500)
  for (let i = 0; i < 10; i++) {
    await instanceA.query(
      `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [cycle, 'A', 'pre-hmr', i, `cycle${cycle}-A-pre-${i}-${padding}`]
    )
  }
  process.send('instance-a-wrote')

  // ---- Phase 2: Attempt to open instance B (the "new module" after HMR) ----
  // With file locking, this should FAIL — which is the correct behavior.
  // In a real dev server, the lock prevents corruption from overlapping instances.
  let instanceBOpened = false
  try {
    const instanceB = new PGlite(dataDir)
    await instanceB.waitReady
    instanceBOpened = true
    process.send('instance-b-ready')

    // If we somehow got here (no lock), do overlap operations
    for (let i = 0; i < overlapOps; i++) {
      await instanceB.query(
        `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [cycle, 'B', 'post-hmr', i, `cycle${cycle}-B-post-${i}-${padding}`]
      )
    }
    process.send('overlap-done')
  } catch (err) {
    // Lock prevented instance B — this is the EXPECTED behavior
    process.send('instance-b-blocked')
    process.send(`lock-error:${err.message}`)
  }

  // Instance A continues writing (it holds the lock)
  for (let i = 10; i < 20; i++) {
    await instanceA.query(
      `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [cycle, 'A', 'post-hmr', i, `cycle${cycle}-A-post-${i}-${padding}`]
    )
  }
  process.send('instance-a-continued')

  process.send('all-operations-done')

  // Keep the process alive so the parent can kill it
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`HMR worker cycle ${cycle} error:`, err)
  try {
    process.send(`fatal:${err.message}`)
  } catch (_) {}
  process.exit(1)
})
