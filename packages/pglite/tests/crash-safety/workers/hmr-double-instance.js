/**
 * Worker: HMR (Hot Module Reload) double-instance simulation
 *
 * Simulates what happens during HMR in dev servers like Vite/Next.js:
 *   1. Opens PGlite instance A on the data dir, does operations
 *   2. WITHOUT closing instance A, opens PGlite instance B on the SAME data dir
 *   3. Instance B does operations while instance A's WASM state is still in memory
 *   4. The process is killed externally, so neither instance gets close() called
 *
 * This is the exact pattern that causes corruption in real-world dev usage:
 * when a module is hot-reloaded, the old module's PGlite instance is garbage
 * collected without close() ever being called, while the new module creates
 * a fresh PGlite instance on the same data directory.
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

  // Instance A does some writes (simulating the "old module" mid-operation)
  const padding = 'A'.repeat(500)
  for (let i = 0; i < 10; i++) {
    await instanceA.query(
      `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [cycle, 'A', 'pre-hmr', i, `cycle${cycle}-A-pre-${i}-${padding}`]
    )
  }
  process.send('instance-a-wrote')

  // ---- Phase 2: Open instance B (the "new module" after HMR) ----
  // CRITICAL: Instance A is NOT closed. This is the HMR bug.
  // In a real dev server, the old module is simply dereferenced and left
  // for garbage collection. The PGlite instance and its WASM heap stay
  // alive until the GC runs, which may be never during the process lifetime.

  const instanceB = new PGlite(dataDir)
  await instanceB.waitReady
  process.send('instance-b-ready')

  // ---- Phase 3: Both instances exist simultaneously ----
  // Instance B does operations while instance A's WASM state is still in memory.
  // This is the danger zone: two separate WASM heaps, two sets of shared buffers,
  // both pointing at the same filesystem data directory.

  for (let i = 0; i < overlapOps; i++) {
    // Instance B writes
    await instanceB.query(
      `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
       VALUES ($1, $2, $3, $4, $5)`,
      [cycle, 'B', 'post-hmr', i, `cycle${cycle}-B-post-${i}-${padding}`]
    )

    // Occasionally also do reads on instance B to exercise the buffer cache
    if (i % 5 === 0) {
      await instanceB.query(`SELECT count(*) FROM hmr_data`)
      await instanceB.query(`SELECT * FROM hmr_data ORDER BY id DESC LIMIT 5`)
    }

    // Every few iterations, do an UPDATE on B (which dirties pages that A
    // might also have cached in its WASM heap)
    if (i % 7 === 0) {
      await instanceB.query(
        `UPDATE hmr_data SET payload = payload || $1
         WHERE instance = 'B' AND cycle = $2 AND seq = $3`,
        ['-updated', cycle, i]
      )
    }
  }

  process.send('overlap-done')

  // ---- Phase 4: Instance A does MORE writes (stale WASM heap) ----
  // This is especially dangerous: instance A's buffer cache is stale
  // (instance B has been writing to the same files on disk), but instance A
  // doesn't know that. Its WASM heap still has the old page images.
  try {
    for (let i = 10; i < 20; i++) {
      await instanceA.query(
        `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [cycle, 'A', 'post-hmr-stale', i, `cycle${cycle}-A-stale-${i}-${padding}`]
      )
    }
    process.send('instance-a-stale-writes-done')
  } catch (err) {
    // Instance A writing after B has modified the same files may error
    process.send(`instance-a-stale-error:${err.message}`)
  }

  // ---- Phase 5: Instance B does a DELETE + re-INSERT (more page churn) ----
  try {
    await instanceB.query(
      `DELETE FROM hmr_data WHERE instance = 'A' AND cycle = $1 AND seq < 5`,
      [cycle]
    )
    for (let i = 0; i < 5; i++) {
      await instanceB.query(
        `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [cycle, 'B', 'replace', i, `cycle${cycle}-B-replace-${i}-${padding}`]
      )
    }
    process.send('instance-b-replace-done')
  } catch (err) {
    process.send(`instance-b-replace-error:${err.message}`)
  }

  // Signal that the overlap phase is fully done; the parent will SIGKILL us
  // WITHOUT either instance being closed. This simulates the dev killing
  // their dev server with Ctrl+C or the process being killed by the OS.
  process.send('all-operations-done')

  // Keep the process alive so the parent can kill it
  // (neither instance A nor B is closed)
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`HMR worker cycle ${cycle} error:`, err)
  try {
    process.send(`fatal:${err.message}`)
  } catch (_) {}
  process.exit(1)
})
