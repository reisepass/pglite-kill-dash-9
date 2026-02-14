/**
 * Crash Safety Test: HMR (Hot Module Reload) Double Instance
 *
 * This is the most realistic simulation of how PGlite gets corrupted in
 * daily dev usage. When a dev server like Vite or Next.js performs HMR:
 *
 *   1. The old module is dereferenced (but NOT cleaned up)
 *   2. The new module is loaded and creates a new PGlite instance
 *   3. The old PGlite instance's WASM heap is still in memory
 *   4. Both instances share the same filesystem data directory
 *   5. The old instance never gets close() called (no checkpoint)
 *   6. Eventually the dev kills the process (Ctrl+C, process exit)
 *
 * This test runs multiple cycles of this pattern, each time:
 *   - Opening instance A, doing writes
 *   - Opening instance B on the SAME dir WITHOUT closing A
 *   - Both instances do overlapping writes
 *   - SIGKILL the process (no close() on either)
 *   - Reopen and verify integrity
 *
 * The key corruption vector: instance A's WASM heap holds dirty buffer pages
 * that don't match what's on disk (since instance B has been writing), and
 * neither instance gets a clean shutdown checkpoint.
 */

import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import { existsSync, rmSync } from 'node:fs'

const dataDir = `/tmp/pglite-crash-hmr-double-${Date.now()}`

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    if (existsSync(dataDir)) {
      rmSync(dataDir, { recursive: true, force: true })
    }
  }
})

describe('crash safety: HMR double-instance corruption', () => {
  it(
    'should survive multiple HMR-style instance replacement cycles with SIGKILL',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const workerPath = new URL(
        './workers/hmr-double-instance.js',
        import.meta.url,
      ).pathname

      const NUM_CYCLES = 7
      const cycleResults = []

      for (let cycle = 0; cycle < NUM_CYCLES; cycle++) {
        // Vary the overlap intensity per cycle to hit different timing windows
        const overlapOps = cycle < 2 ? 10 : (cycle < 5 ? 25 : 40)

        const result = await new Promise((resolve) => {
          const messages = []
          let killed = false

          const child = fork(workerPath, [], {
            env: {
              ...process.env,
              PGLITE_DATA_DIR: dataDir,
              CYCLE: String(cycle),
              OVERLAP_OPS: String(overlapOps),
            },
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
          })

          let stderr = ''
          child.stderr.on('data', (d) => { stderr += d.toString() })
          child.stdout.on('data', () => {}) // drain

          child.on('message', (msg) => {
            messages.push(msg)

            // Kill when the overlap phase is fully done (both instances
            // have been writing, maximum dirty state) OR when we've seen
            // enough operations. For early cycles, kill mid-overlap for
            // extra aggression.
            if (cycle < 3) {
              // Kill mid-overlap for cycles 0-2 (most aggressive)
              if (msg === 'overlap-done') {
                if (!killed) {
                  killed = true
                  child.kill('SIGKILL')
                }
              }
            } else {
              // Kill after all operations for later cycles
              if (msg === 'all-operations-done') {
                if (!killed) {
                  killed = true
                  child.kill('SIGKILL')
                }
              }
            }
          })

          child.on('exit', (code, sig) => {
            resolve({
              cycle,
              killed: killed || sig === 'SIGKILL',
              messages,
              exitCode: code,
              signal: sig,
              stderr,
            })
          })

          // Fallback timer: if the worker takes too long (e.g., it hangs
          // because the second instance can't open), kill after 15s
          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill('SIGKILL')
            }
          }, 15000)

          // Safety timeout
          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill('SIGKILL')
            }
          }, 30000)
        })

        cycleResults.push(result)

        // Verify the worker was killed (not exited cleanly)
        expect(result.killed).toBe(true)

        // Instance A should have been ready in every cycle
        expect(result.messages).toContain('instance-a-ready')

        // On the first cycle, schema should have been created
        if (cycle === 0) {
          expect(result.messages).toContain('schema-created')
        }

        // Log cycle details for debugging
        console.log(
          `Cycle ${cycle}: messages=[${result.messages.join(', ')}], ` +
          `exit=${result.exitCode}, signal=${result.signal}`
        )

        // ---- Intermediate recovery check every 2 cycles ----
        // Try to open the DB after the crash and verify it's still usable.
        // This catches progressive corruption that builds up over cycles.
        if (cycle % 2 === 1 || cycle === NUM_CYCLES - 1) {
          let db = null
          let openSuccess = false
          try {
            db = new PGlite(dataDir)
            await Promise.race([
              db.waitReady,
              new Promise((_, reject) =>
                setTimeout(
                  () => reject(new Error(`Open timed out after cycle ${cycle}`)),
                  20000,
                ),
              ),
            ])
            openSuccess = true
          } catch (err) {
            console.log(`CORRUPTION DETECTED after cycle ${cycle}: ${err.message}`)
            openSuccess = false
          }

          if (openSuccess && db) {
            try {
              // Basic health check
              await db.query('SELECT 1 as ok')

              // Table should exist after cycle 0
              const tableCheck = await db.query(`
                SELECT tablename FROM pg_tables
                WHERE schemaname = 'public' AND tablename = 'hmr_data'
              `)
              expect(tableCheck.rows.length).toBe(1)

              // Count rows - should have some data
              const countResult = await db.query(
                'SELECT count(*)::int as cnt FROM hmr_data'
              )
              const rowCount = countResult.rows[0].cnt
              console.log(`After cycle ${cycle}: ${rowCount} rows in hmr_data`)
              expect(rowCount).toBeGreaterThan(0)

              // Verify index is usable
              const indexScan = await db.query(
                `SELECT count(*)::int as cnt FROM hmr_data WHERE cycle = $1`,
                [0]
              )
              expect(indexScan.rows[0].cnt).toBeGreaterThanOrEqual(0)

              // Verify we can do a full sequential scan without errors
              const allRows = await db.query(
                'SELECT id, cycle, instance, phase, seq FROM hmr_data ORDER BY id'
              )
              expect(allRows.rows.length).toBe(rowCount)

              // Check for data consistency: each row should have valid fields
              for (const row of allRows.rows) {
                expect(row.id).toBeGreaterThan(0)
                expect(row.cycle).toBeGreaterThanOrEqual(0)
                expect(row.cycle).toBeLessThanOrEqual(cycle)
                expect(['A', 'B']).toContain(row.instance)
                expect(typeof row.phase).toBe('string')
                expect(row.seq).toBeGreaterThanOrEqual(0)
              }
            } finally {
              await db.close()
            }
          }

          // The DB MUST be openable - if not, we found the corruption bug
          expect(openSuccess).toBe(true)
        }

        // Small delay between cycles to let filesystem settle
        await new Promise((r) => setTimeout(r, 200))
      }

      // ---- Final comprehensive verification ----
      console.log('\n--- Final verification after all HMR cycles ---')

      let finalDb = null
      try {
        finalDb = new PGlite(dataDir)
        await Promise.race([
          finalDb.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Final open timed out')), 30000),
          ),
        ])
      } catch (err) {
        console.log(`FINAL CORRUPTION: ${err.message}`)
        expect.fail(`Database corrupted after ${NUM_CYCLES} HMR cycles: ${err.message}`)
      }

      try {
        // Full integrity check
        const tables = await finalDb.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        `)
        expect(tables.rows.length).toBeGreaterThanOrEqual(1)

        // Check all indexes are usable
        const indexes = await finalDb.query(`
          SELECT indexname, tablename FROM pg_indexes
          WHERE schemaname = 'public'
        `)
        for (const idx of indexes.rows) {
          await finalDb.query(
            `SELECT count(*) FROM "${idx.tablename}"`
          )
        }

        // Row count audit
        const finalCount = await finalDb.query(
          'SELECT count(*)::int as cnt FROM hmr_data'
        )
        console.log(`Final row count: ${finalCount.rows[0].cnt}`)

        // Per-cycle breakdown
        const cycleBreakdown = await finalDb.query(`
          SELECT cycle, instance, count(*)::int as cnt
          FROM hmr_data
          GROUP BY cycle, instance
          ORDER BY cycle, instance
        `)
        console.log('Per-cycle breakdown:')
        for (const row of cycleBreakdown.rows) {
          console.log(`  cycle=${row.cycle} instance=${row.instance} count=${row.cnt}`)
        }

        // Verify we can still write to the database
        await finalDb.query(
          `INSERT INTO hmr_data (cycle, instance, phase, seq, payload)
           VALUES ($1, $2, $3, $4, $5)`,
          [999, 'verify', 'final', 0, 'final-verification-row']
        )
        const verifyRow = await finalDb.query(
          `SELECT * FROM hmr_data WHERE cycle = 999`
        )
        expect(verifyRow.rows.length).toBe(1)
      } finally {
        await finalDb.close()
      }
    },
    { timeout: 300000 },
  )

  it(
    'should survive rapid HMR cycles with minimal delay between instance swaps',
    async () => {
      const { PGlite } = await import('../../dist/index.js')

      // Use a separate data dir for this sub-test
      const rapidDataDir = `${dataDir}-rapid`

      // This test is more extreme: we open instance B almost immediately
      // after instance A starts, giving maximum overlap time.
      const workerPath = new URL(
        './workers/hmr-double-instance.js',
        import.meta.url,
      ).pathname

      const RAPID_CYCLES = 5

      for (let cycle = 0; cycle < RAPID_CYCLES; cycle++) {
        const result = await new Promise((resolve) => {
          const messages = []
          let killed = false

          const child = fork(workerPath, [], {
            env: {
              ...process.env,
              PGLITE_DATA_DIR: rapidDataDir,
              CYCLE: String(cycle),
              // Very high overlap to maximize concurrent dirty pages
              OVERLAP_OPS: '50',
            },
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
          })

          let stderr = ''
          child.stderr.on('data', (d) => { stderr += d.toString() })
          child.stdout.on('data', () => {})

          child.on('message', (msg) => {
            messages.push(msg)

            // Kill as soon as instance B is ready (maximum overlap,
            // neither instance has done much work after B opened)
            if (msg === 'instance-b-ready' && !killed) {
              // Give a tiny window for B to start writing, then kill
              setTimeout(() => {
                if (!killed) {
                  killed = true
                  child.kill('SIGKILL')
                }
              }, 50)
            }
          })

          child.on('exit', (code, sig) => {
            resolve({ cycle, killed: killed || sig === 'SIGKILL', messages, stderr })
          })

          setTimeout(() => {
            if (!killed) { killed = true; child.kill('SIGKILL') }
          }, 15000)
        })

        expect(result.killed).toBe(true)
      }

      // Verify the DB is still usable after rapid HMR cycles
      let db = null
      try {
        db = new PGlite(rapidDataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Rapid HMR final open timed out')), 20000),
          ),
        ])

        await db.query('SELECT 1')

        const tableExists = await db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename = 'hmr_data'
        `)
        if (tableExists.rows.length > 0) {
          const count = await db.query('SELECT count(*)::int as cnt FROM hmr_data')
          console.log(`Rapid HMR test: ${count.rows[0].cnt} rows survived`)
          expect(count.rows[0].cnt).toBeGreaterThanOrEqual(0)
        }

        await db.close()
      } catch (err) {
        console.log(`Rapid HMR CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Rapid HMR corrupted DB: ${err.message}`)
      } finally {
        if (!process.env.RETAIN_DATA && existsSync(rapidDataDir)) {
          rmSync(rapidDataDir, { recursive: true, force: true })
        }
      }
    },
    { timeout: 180000 },
  )
})
