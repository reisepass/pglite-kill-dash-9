/**
 * Crash Safety Test: Overlapping Instances
 *
 * PGlite has NO file locking (GitHub issue #85). Multiple instances can open
 * the same data directory simultaneously, each with their own WASM heap and
 * buffer cache. When killed, the stale caches cause corruption.
 *
 * This test covers scenarios NOT covered by hmr-double-instance.test.js:
 *   1. Three simultaneous instances writing concurrently
 *   2. Staggered overlap (B opens while A is mid-write)
 *   3. DDL collision (one instance does CREATE TABLE / ALTER TABLE while another does INSERTs)
 *   4. Rapid instance cycling (10 instances opened without closing, then SIGKILL)
 *   5. Two instances racing to recover the same dirty data directory
 *
 * These tests EXPECT corruption. They document the vulnerability caused by
 * the lack of file locking. When file locking is implemented, these tests
 * should be updated to verify that locking prevents the corruption.
 */

import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import { existsSync, rmSync } from 'node:fs'

const baseDir = `/tmp/pglite-crash-overlapping-${Date.now()}`

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    for (const suffix of ['triple', 'stagger', 'ddl', 'rapid']) {
      const dir = `${baseDir}-${suffix}`
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true })
      }
    }
  }
})

/**
 * Helper: fork a worker, collect messages, kill on trigger or timer.
 * Returns result with `crashed` flag (true if worker aborted/errored before kill).
 */
function runWorker({ workerPath, dataDir, env = {}, killOnMessage = null, killAfterMs = null, killDelayAfterMsg = 0 }) {
  return new Promise((resolve) => {
    const messages = []
    let killed = false
    let killTimer = null

    const child = fork(workerPath, [], {
      env: { ...process.env, PGLITE_DATA_DIR: dataDir, ...env },
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    })

    let stderr = ''
    child.stderr.on('data', (d) => { stderr += d.toString() })
    child.stdout.on('data', () => {})

    child.on('message', (msg) => {
      messages.push(msg)

      if (killOnMessage && msg === killOnMessage && !killed) {
        if (killDelayAfterMsg > 0) {
          setTimeout(() => {
            if (!killed) { killed = true; child.kill('SIGKILL') }
          }, killDelayAfterMsg)
        } else {
          killed = true
          child.kill('SIGKILL')
        }
      }
    })

    child.on('exit', (code, sig) => {
      if (killTimer) clearTimeout(killTimer)
      const wasKilled = killed || sig === 'SIGKILL'
      const crashed = !wasKilled && code !== 0
      resolve({
        killed: wasKilled,
        crashed,
        terminated: wasKilled || crashed, // worker didn't exit cleanly
        messages,
        exitCode: code,
        signal: sig,
        stderr,
      })
    })

    if (killAfterMs) {
      killTimer = setTimeout(() => {
        if (!killed) { killed = true; child.kill('SIGKILL') }
      }, killAfterMs)
    }

    // Safety timeout
    setTimeout(() => {
      if (!killed) { killed = true; child.kill('SIGKILL') }
    }, 30000)
  })
}

/**
 * Try to open a PGlite instance and run basic checks.
 * Returns { success, error, rowCount }.
 */
async function tryOpenAndVerify(PGlite, dataDir, tableName, timeoutMs = 20000) {
  let db = null
  try {
    db = new PGlite(dataDir)
    await Promise.race([
      db.waitReady,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`Open timed out after ${timeoutMs}ms`)), timeoutMs)
      ),
    ])

    // Basic health
    await db.query('SELECT 1 as ok')

    // Check table exists
    const tableCheck = await db.query(
      `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`,
      [tableName]
    )
    if (tableCheck.rows.length === 0) {
      await db.close()
      return { success: true, rowCount: 0 }
    }

    // Count rows
    const count = await db.query(`SELECT count(*)::int as cnt FROM "${tableName}"`)
    const rowCount = count.rows[0].cnt

    // Full sequential scan
    const allRows = await db.query(`SELECT * FROM "${tableName}" ORDER BY id`)
    if (allRows.rows.length !== rowCount) {
      await db.close()
      return { success: false, error: new Error(`Row count mismatch: count=${rowCount} scan=${allRows.rows.length}`) }
    }

    await db.close()
    return { success: true, rowCount }
  } catch (err) {
    if (db) try { await db.close() } catch (_) {}
    return { success: false, error: err }
  }
}

describe('crash safety: overlapping instances corruption', () => {

  // ========================================================================
  // Scenario 1: Three simultaneous instances
  // ========================================================================
  it(
    'should survive three simultaneous instances all writing then SIGKILL',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${baseDir}-triple`
      const workerPath = new URL(
        './workers/overlapping-three-instances.js',
        import.meta.url,
      ).pathname

      const NUM_CYCLES = 5
      let corruptionDetected = false
      let corruptionDetails = []

      for (let cycle = 0; cycle < NUM_CYCLES; cycle++) {
        // Vary kill timing
        const killMsg = cycle < 2 ? 'concurrent-writes-done' : 'all-done'

        const result = await runWorker({
          workerPath,
          dataDir,
          env: { CYCLE: String(cycle) },
          killOnMessage: killMsg,
        })

        console.log(
          `Triple cycle ${cycle}: terminated=${result.terminated} killed=${result.killed} crashed=${result.crashed} ` +
          `messages=[${result.messages.join(', ')}]`
        )

        // Worker should have been terminated (killed or crashed from WASM abort)
        expect(result.terminated).toBe(true)

        // On cycle 0, instance A must have been ready and schema created
        if (cycle === 0 && !result.crashed) {
          expect(result.messages).toContain('instance-a-ready')
          expect(result.messages).toContain('schema-created')
        }

        // If worker crashed (WASM abort), that's corruption in the worker itself
        if (result.crashed) {
          corruptionDetected = true
          corruptionDetails.push(`Triple cycle ${cycle}: worker crashed (WASM abort)`)
        }

        // Verify recovery
        const verify = await tryOpenAndVerify(PGlite, dataDir, 'triple_data')
        if (!verify.success) {
          corruptionDetected = true
          corruptionDetails.push(`Triple cycle ${cycle}: DB failed to open: ${verify.error.message}`)
          console.log(`CORRUPTION after triple cycle ${cycle}: ${verify.error.message}`)
          // Once corrupted, stop cycling - further cycles will just fail
          break
        } else {
          console.log(`  Triple cycle ${cycle}: ${verify.rowCount} rows, DB OK`)
        }

        await new Promise((r) => setTimeout(r, 200))
      }

      // Report findings
      if (corruptionDetected) {
        console.log('\n=== CORRUPTION CONFIRMED (triple instances) ===')
        for (const detail of corruptionDetails) {
          console.log(`  ${detail}`)
        }
      }

      // This test expects corruption to be detected (proving the vulnerability)
      expect(corruptionDetected).toBe(true)
    },
    { timeout: 300000 },
  )

  // ========================================================================
  // Scenario 2: Staggered overlap (B opens while A is mid-write)
  // ========================================================================
  it(
    'should survive staggered overlapping instances over multiple cycles',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${baseDir}-stagger`
      const workerPath = new URL(
        './workers/overlapping-staggered.js',
        import.meta.url,
      ).pathname

      const NUM_CYCLES = 7
      let corruptionDetected = false
      let corruptionDetails = []

      for (let cycle = 0; cycle < NUM_CYCLES; cycle++) {
        const staggerMs = cycle < 3 ? 100 : (cycle < 5 ? 300 : 500)
        const killMsg = cycle < 3 ? 'overlap-writes-done' : 'all-done'

        const result = await runWorker({
          workerPath,
          dataDir,
          env: { CYCLE: String(cycle), STAGGER_MS: String(staggerMs) },
          killOnMessage: killMsg,
        })

        console.log(
          `Stagger cycle ${cycle} (${staggerMs}ms): terminated=${result.terminated} ` +
          `messages=[${result.messages.join(', ')}]`
        )

        expect(result.terminated).toBe(true)

        if (result.crashed) {
          corruptionDetected = true
          corruptionDetails.push(`Stagger cycle ${cycle}: worker crashed`)
        }

        // Check recovery every 2 cycles and at the end
        if (cycle % 2 === 1 || cycle === NUM_CYCLES - 1) {
          const verify = await tryOpenAndVerify(PGlite, dataDir, 'stagger_data')
          if (!verify.success) {
            corruptionDetected = true
            corruptionDetails.push(`Stagger cycle ${cycle}: DB open failed: ${verify.error.message}`)
            console.log(`CORRUPTION after stagger cycle ${cycle}: ${verify.error.message}`)
            break
          } else {
            console.log(`  Stagger cycle ${cycle}: ${verify.rowCount} rows, DB OK`)
          }
        }

        await new Promise((r) => setTimeout(r, 200))
      }

      if (corruptionDetected) {
        console.log('\n=== CORRUPTION CONFIRMED (staggered overlap) ===')
        for (const detail of corruptionDetails) {
          console.log(`  ${detail}`)
        }
      }

      expect(corruptionDetected).toBe(true)
    },
    { timeout: 300000 },
  )

  // ========================================================================
  // Scenario 3: DDL collision (CREATE TABLE/INDEX vs INSERTs/UPDATEs)
  // ========================================================================
  it(
    'should survive DDL operations on one process colliding with DML on another process',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${baseDir}-ddl`
      const ddlWorkerPath = new URL(
        './workers/overlapping-ddl-writer.js',
        import.meta.url,
      ).pathname

      const NUM_CYCLES = 5
      let corruptionDetected = false
      let corruptionDetails = []

      for (let cycle = 0; cycle < NUM_CYCLES; cycle++) {
        // Spawn TWO separate child processes on the SAME data dir:
        // one doing DDL (CREATE TABLE, ALTER, indexes) and one doing DML (inserts/updates)
        // This is two separate WASM heaps, two separate Emscripten filesystems.
        const [ddlResult, dmlResult] = await Promise.all([
          runWorker({
            workerPath: ddlWorkerPath,
            dataDir,
            env: { CYCLE: String(cycle), WRITER_MODE: 'ddl' },
            killAfterMs: cycle < 2 ? 3000 : 5000,
          }),
          runWorker({
            workerPath: ddlWorkerPath,
            dataDir,
            env: { CYCLE: String(cycle), WRITER_MODE: 'dml' },
            killAfterMs: cycle < 2 ? 3000 : 5000,
          }),
        ])

        console.log(
          `DDL cycle ${cycle}: DDL terminated=${ddlResult.terminated} crashed=${ddlResult.crashed} ` +
          `messages=[${ddlResult.messages.join(', ')}]`
        )
        console.log(
          `DDL cycle ${cycle}: DML terminated=${dmlResult.terminated} crashed=${dmlResult.crashed} ` +
          `messages=[${dmlResult.messages.join(', ')}]`
        )

        if (ddlResult.crashed) {
          corruptionDetected = true
          corruptionDetails.push(`DDL cycle ${cycle}: DDL worker crashed`)
        }
        if (dmlResult.crashed) {
          corruptionDetected = true
          corruptionDetails.push(`DDL cycle ${cycle}: DML worker crashed`)
        }

        // Verify recovery
        await new Promise((r) => setTimeout(r, 300))
        const verify = await tryOpenAndVerify(PGlite, dataDir, 'ddl_base')
        if (!verify.success) {
          corruptionDetected = true
          corruptionDetails.push(`DDL cycle ${cycle}: DB open failed: ${verify.error.message}`)
          console.log(`CORRUPTION after DDL cycle ${cycle}: ${verify.error.message}`)
          break
        } else {
          console.log(`  DDL cycle ${cycle}: ddl_base has ${verify.rowCount} rows, DB OK`)
        }

        await new Promise((r) => setTimeout(r, 200))
      }

      if (corruptionDetected) {
        console.log('\n=== CORRUPTION CONFIRMED (DDL collision) ===')
        for (const detail of corruptionDetails) {
          console.log(`  ${detail}`)
        }
      }

      expect(corruptionDetected).toBe(true)
    },
    { timeout: 300000 },
  )

  // ========================================================================
  // Scenario 4: Rapid instance cycling (10 instances, no close, SIGKILL)
  // ========================================================================
  it(
    'should survive rapid instance cycling with 10 unclosed instances then SIGKILL',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${baseDir}-rapid`
      const workerPath = new URL(
        './workers/overlapping-rapid-cycling.js',
        import.meta.url,
      ).pathname

      const NUM_RUNS = 3
      let corruptionDetected = false
      let corruptionDetails = []

      for (let run = 0; run < NUM_RUNS; run++) {
        const numInstances = run === 0 ? 8 : (run === 1 ? 10 : 12)
        const killMsg = run < 2 ? 'all-instances-created' : 'all-done'

        const result = await runWorker({
          workerPath,
          dataDir,
          env: { NUM_INSTANCES: String(numInstances) },
          killOnMessage: killMsg,
        })

        const instWrote = result.messages.filter(m => m.endsWith('-wrote')).length
        console.log(
          `Rapid run ${run} (${numInstances} instances): terminated=${result.terminated} ` +
          `${instWrote} wrote, messages=[${result.messages.join(', ')}]`
        )

        expect(result.terminated).toBe(true)

        if (result.crashed) {
          corruptionDetected = true
          corruptionDetails.push(`Rapid run ${run}: worker crashed`)
        }

        // Verify recovery
        const verify = await tryOpenAndVerify(PGlite, dataDir, 'rapid_data')
        if (!verify.success) {
          corruptionDetected = true
          corruptionDetails.push(`Rapid run ${run}: DB open failed: ${verify.error.message}`)
          console.log(`CORRUPTION after rapid run ${run}: ${verify.error.message}`)
          break
        } else {
          console.log(`  Rapid run ${run}: ${verify.rowCount} rows, DB OK`)
        }

        await new Promise((r) => setTimeout(r, 300))
      }

      if (corruptionDetected) {
        console.log('\n=== CORRUPTION CONFIRMED (rapid cycling) ===')
        for (const detail of corruptionDetails) {
          console.log(`  ${detail}`)
        }
      }

      expect(corruptionDetected).toBe(true)
    },
    { timeout: 300000 },
  )

  // ========================================================================
  // Scenario 5: Kill-during-recovery overlap (two instances both recovering)
  // ========================================================================
  it(
    'should survive two instances racing to recover the same dirty data directory',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${baseDir}-triple`  // Reuse from scenario 1 (already corrupted)
      let corruptionDetected = false
      let corruptionDetails = []

      const workerPath = new URL(
        './workers/overlapping-three-instances.js',
        import.meta.url,
      ).pathname

      // Step 1: Create dirty state (if not already corrupted from scenario 1)
      const seedResult = await runWorker({
        workerPath,
        dataDir,
        env: { CYCLE: '99' },
        killOnMessage: 'concurrent-writes-done',
        killAfterMs: 8000, // fallback timer if message never arrives (data dir already corrupted)
      })

      if (seedResult.crashed) {
        corruptionDetected = true
        corruptionDetails.push('Seed worker crashed (data dir already corrupted)')
      }
      console.log(`Recovery-overlap seed: terminated=${seedResult.terminated} crashed=${seedResult.crashed}`)

      // Step 2: Spawn TWO workers simultaneously on the dirty data dir
      // Both will try to open (triggering WAL recovery), creating a race
      const [recoverA, recoverB] = await Promise.all([
        runWorker({
          workerPath,
          dataDir,
          env: { CYCLE: '100' },
          killAfterMs: 5000,
        }),
        new Promise(resolve => setTimeout(resolve, 200)).then(() =>
          runWorker({
            workerPath,
            dataDir,
            env: { CYCLE: '101' },
            killAfterMs: 5000,
          })
        ),
      ])

      console.log(`Recovery A: terminated=${recoverA.terminated} crashed=${recoverA.crashed} messages=[${recoverA.messages.join(', ')}]`)
      console.log(`Recovery B: terminated=${recoverB.terminated} crashed=${recoverB.crashed} messages=[${recoverB.messages.join(', ')}]`)

      if (recoverA.crashed) {
        corruptionDetected = true
        corruptionDetails.push(`Recovery instance A crashed: ${recoverA.stderr.slice(0, 200)}`)
      }
      if (recoverB.crashed) {
        corruptionDetected = true
        corruptionDetails.push(`Recovery instance B crashed: ${recoverB.stderr.slice(0, 200)}`)
      }

      // Step 3: Try to open after the dual-recovery race
      await new Promise((r) => setTimeout(r, 500))

      const verify = await tryOpenAndVerify(PGlite, dataDir, 'triple_data')
      if (!verify.success) {
        corruptionDetected = true
        corruptionDetails.push(`Final open failed: ${verify.error.message}`)
        console.log(`CORRUPTION after recovery overlap: ${verify.error.message}`)
      } else {
        console.log(`  After recovery overlap: ${verify.rowCount} rows, DB OK`)
      }

      if (corruptionDetected) {
        console.log('\n=== CORRUPTION CONFIRMED (recovery overlap) ===')
        for (const detail of corruptionDetails) {
          console.log(`  ${detail}`)
        }
      }

      expect(corruptionDetected).toBe(true)
    },
    { timeout: 180000 },
  )
})
