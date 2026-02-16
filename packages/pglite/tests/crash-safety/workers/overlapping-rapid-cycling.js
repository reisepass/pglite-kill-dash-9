/**
 * Worker: Rapid instance cycling without close
 *
 * Opens an instance, writes 1 row, does NOT close it, opens a new instance
 * on the same dir, writes 1 row, does NOT close it, repeat N times.
 * All instances stay open in memory simultaneously.
 *
 * This simulates the worst case of a dev server that rapidly reloads:
 * each reload creates a new PGlite instance, none are ever closed, and
 * they all hold stale WASM heaps pointing at the same filesystem.
 *
 * Environment variables:
 *   PGLITE_DATA_DIR  - path to the data directory
 *   NUM_INSTANCES    - how many instances to open (default 10)
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const numInstances = parseInt(process.env.NUM_INSTANCES || '10', 10)

async function run() {
  const instances = []
  const pad = 'R'.repeat(500)

  // Open the first instance and create the schema
  const first = new PGlite(dataDir)
  await first.waitReady
  instances.push(first)

  await first.query(`
    CREATE TABLE IF NOT EXISTS rapid_data (
      id SERIAL PRIMARY KEY,
      instance_num INTEGER NOT NULL,
      seq INTEGER NOT NULL,
      payload TEXT NOT NULL
    )
  `)
  await first.query(`CREATE INDEX IF NOT EXISTS idx_rapid_inst ON rapid_data (instance_num)`)
  process.send('schema-created')

  // First instance writes a row
  await first.query(
    `INSERT INTO rapid_data (instance_num, seq, payload) VALUES ($1, $2, $3)`,
    [0, 0, `inst-0-row-0-${pad}`]
  )
  process.send('instance-0-wrote')

  // Now rapidly open new instances, each writing a row, never closing previous ones
  for (let i = 1; i < numInstances; i++) {
    const inst = new PGlite(dataDir)
    await inst.waitReady
    instances.push(inst)

    // Write a row from this new instance
    await inst.query(
      `INSERT INTO rapid_data (instance_num, seq, payload) VALUES ($1, $2, $3)`,
      [i, 0, `inst-${i}-row-0-${pad}`]
    )
    process.send(`instance-${i}-wrote`)

    // Also have an older instance write (stale cache scenario)
    if (instances.length > 2) {
      const oldIdx = Math.floor(Math.random() * (instances.length - 1))
      try {
        await instances[oldIdx].query(
          `INSERT INTO rapid_data (instance_num, seq, payload) VALUES ($1, $2, $3)`,
          [oldIdx, i, `inst-${oldIdx}-stale-write-from-cycle-${i}-${pad}`]
        )
      } catch (err) {
        process.send(`stale-write-error-${oldIdx}:${err.message}`)
      }
    }
  }

  process.send('all-instances-created')

  // Final burst: have ALL instances try to write simultaneously
  try {
    await Promise.all(
      instances.map((inst, idx) =>
        inst.query(
          `INSERT INTO rapid_data (instance_num, seq, payload) VALUES ($1, $2, $3)`,
          [idx, 999, `inst-${idx}-final-burst-${pad}`]
        )
      )
    )
    process.send('final-burst-done')
  } catch (err) {
    process.send(`final-burst-error:${err.message}`)
  }

  process.send('all-done')

  // Keep alive - none of the instances are closed
  await new Promise(() => {})
}

run().catch((err) => {
  console.error(`Rapid cycling worker error:`, err)
  try { process.send(`fatal:${err.message}`) } catch (_) {}
  process.exit(1)
})
