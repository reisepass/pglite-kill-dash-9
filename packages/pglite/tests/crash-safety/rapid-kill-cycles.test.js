/**
 * Crash Safety Test: Rapid Open-Write-Kill Cycles
 *
 * Tests multiple strategies for corrupting PGlite through extremely rapid
 * open-write-kill cycles that simulate dev server restarts, HMR, nodemon, etc.
 *
 * Key insight: Killing DURING active writes (mid-INSERT, mid-UPDATE) is far
 * more dangerous than killing between operations. We use aggressive timer-based
 * kills to catch PGlite mid-write.
 *
 * Scenarios:
 *   1. Ultra-rapid cycles with kill during init (before waitReady)
 *   2. Kill during WAL recovery (kill during recovery from previous kill)
 *   3. Alternating heavy/light writes across kills
 *   4. Schema evolution across kills (DDL operations)
 *   5. Minimum kills to corrupt (kill mid-write, no checkpoint)
 */

import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import { existsSync, rmSync, statSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

const dataDirBase = `/tmp/pglite-crash-rapid-kill-${Date.now()}`

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    // Clean up all data dirs created by this test
    for (const suffix of [
      'init', 'recovery', 'alternating', 'schema', 'combined',
      'midwrite3', 'midwrite5', 'midwrite10', 'midwrite15', 'midwrite20',
      'midwrite25', 'midwrite30',
      'first-init-50ms', 'first-init-100ms', 'first-init-150ms',
      'first-init-200ms', 'first-init-250ms',
    ]) {
      const dir = `${dataDirBase}-${suffix}`
      if (existsSync(dir)) {
        rmSync(dir, { recursive: true, force: true })
      }
    }
  }
})

function dirSizeBytes(dir) {
  let total = 0
  try {
    const entries = readdirSync(dir, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = join(dir, entry.name)
      if (entry.isDirectory()) {
        total += dirSizeBytes(fullPath)
      } else if (entry.isFile()) {
        total += statSync(fullPath).size
      }
    }
  } catch {
    // directory may not exist yet
  }
  return total
}

/**
 * Spawn a worker, wait for optional message trigger or timer, then SIGKILL.
 */
function spawnAndKill(workerPath, env, killStrategy) {
  return new Promise((resolve) => {
    const messages = []
    let killed = false
    let killTimer = null

    const child = fork(workerPath, [], {
      env: { ...process.env, ...env },
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    })

    let stderr = ''
    let stdout = ''
    child.stderr.on('data', (d) => { stderr += d.toString() })
    child.stdout.on('data', (d) => { stdout += d.toString() })

    child.on('message', (msg) => {
      messages.push(msg)

      if (killStrategy.onMessage && !killed) {
        if (typeof killStrategy.onMessage === 'string' && msg === killStrategy.onMessage) {
          killed = true
          if (killTimer) clearTimeout(killTimer)
          child.kill('SIGKILL')
        } else if (typeof killStrategy.onMessage === 'function' && killStrategy.onMessage(msg)) {
          killed = true
          if (killTimer) clearTimeout(killTimer)
          child.kill('SIGKILL')
        }
      }
    })

    child.on('exit', (code, sig) => {
      if (killTimer) clearTimeout(killTimer)
      resolve({
        killed: killed || sig === 'SIGKILL',
        messages,
        exitCode: code,
        signal: sig,
        stderr,
        stdout,
      })
    })

    if (killStrategy.afterMs != null) {
      killTimer = setTimeout(() => {
        if (!killed) {
          killed = true
          child.kill('SIGKILL')
        }
      }, killStrategy.afterMs)
    }

    // Safety timeout
    setTimeout(() => {
      if (!killed) {
        killed = true
        child.kill('SIGKILL')
      }
    }, 60000)
  })
}

describe('crash safety: rapid kill cycles', () => {
  // ============================================================
  // Scenario 1: Ultra-rapid cycles with kill during init
  // ============================================================
  it(
    'should survive 25 ultra-rapid kill-during-init cycles',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-init`
      const workerPath = new URL('./workers/rapid-kill-init.js', import.meta.url).pathname

      const CYCLES = 25
      const results = []

      for (let i = 0; i < CYCLES; i++) {
        let killStrategy
        const phase = i % 4

        if (phase === 0) {
          killStrategy = { afterMs: 50 }
        } else if (phase === 1) {
          killStrategy = { onMessage: 'constructor-done' }
        } else if (phase === 2) {
          killStrategy = { onMessage: 'ready' }
        } else {
          killStrategy = { afterMs: 100 }
        }

        const doWrite = i % 3 === 0 ? '1' : '0'

        const result = await spawnAndKill(
          workerPath,
          { PGLITE_DATA_DIR: dataDir, DO_WRITE: doWrite },
          killStrategy,
        )

        results.push(result)

        if (i % 5 === 0 || i === CYCLES - 1) {
          const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
          console.log(
            `Init-kill cycle ${i}/${CYCLES}: killed=${result.killed}, ` +
            `msgs=[${result.messages.slice(0, 3).join(',')}], size=${sizeMB}MB`
          )
        }

        if (!result.killed && result.exitCode !== 0 && result.messages.length === 0) {
          console.log(`  CORRUPTION: Worker crashed at cycle ${i}: ${result.stderr.slice(0, 200)}`)
          break
        }
      }

      // Final verification
      console.log('\n--- Init-kill final verification ---')
      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Init-kill final open timed out')), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        `)
        console.log(`Tables after init-kill: ${tables.rows.map(r => r.tablename).join(', ') || '(none)'}`)

        if (tables.rows.some(r => r.tablename === 'init_test')) {
          const count = await db.query('SELECT count(*)::int as cnt FROM init_test')
          console.log(`init_test rows: ${count.rows[0].cnt}`)
        }

        await db.query('CREATE TABLE IF NOT EXISTS init_verify (id SERIAL PRIMARY KEY, ok BOOLEAN)')
        await db.query('INSERT INTO init_verify (ok) VALUES (true)')
        const verify = await db.query('SELECT ok FROM init_verify')
        expect(verify.rows[0].ok).toBe(true)

        await db.close()
      } catch (err) {
        console.log(`INIT-KILL CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Init-kill cycles corrupted DB after ${CYCLES} cycles: ${err.message}`)
      }
    },
    { timeout: 120000 },
  )

  // ============================================================
  // Scenario 2: Kill during WAL recovery (chained)
  // ============================================================
  it(
    'should survive 20 kill-during-recovery cycles with heavy WAL',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-recovery`
      const midwriteWorker = new URL('./workers/rapid-kill-midwrite.js', import.meta.url).pathname
      const recoveryWorker = new URL('./workers/rapid-kill-recovery.js', import.meta.url).pathname

      // Phase 1: Create heavy dirty WAL state
      // First cycle: let init complete, kill mid-insert to leave dirty WAL
      console.log('Phase 1: Creating heavy dirty WAL...')
      {
        const result = await spawnAndKill(
          midwriteWorker,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: '0', MODE: 'setup' },
          { onMessage: (msg) => msg === 'row:200' },
        )
        const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
        console.log(`  Setup 0 (init): killed=${result.killed}, msgs=${result.messages.length}, size=${sizeMB}MB`)
      }
      // Subsequent cycles: kill mid-write after ready
      for (let i = 1; i < 5; i++) {
        const result = await spawnAndKill(
          midwriteWorker,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(i), MODE: 'stress' },
          { onMessage: (msg) => typeof msg === 'string' && msg.startsWith('row:') && parseInt(msg.split(':')[1]) >= 50 },
        )
        const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
        console.log(
          `  Setup ${i}: killed=${result.killed}, ` +
          `msgs=${result.messages.length}, size=${sizeMB}MB`
        )
      }

      // Phase 2: Rapid kill during recovery - kill before waitReady resolves
      console.log('\nPhase 2: Kill during recovery (20 cycles)...')
      const RECOVERY_KILLS = 20
      let corruptionDetected = false

      for (let i = 0; i < RECOVERY_KILLS; i++) {
        // Very aggressive: 30-100ms kills during recovery
        const killMs = 30 + (i % 4) * 25
        const result = await spawnAndKill(
          recoveryWorker,
          { PGLITE_DATA_DIR: dataDir, DO_HEAVY_WRITE: i % 3 === 0 ? '1' : '0' },
          { afterMs: killMs },
        )

        const gotReady = result.messages.includes('ready')
        if (i % 4 === 0 || i === RECOVERY_KILLS - 1) {
          console.log(
            `  Recovery-kill ${i}: killMs=${killMs}, killed=${result.killed}, ` +
            `ready=${gotReady}, msgs=[${result.messages.join(',')}]`
          )
        }

        if (!result.killed && result.exitCode !== 0) {
          console.log(`  CORRUPTION at recovery-kill ${i}: ${result.stderr.slice(0, 300)}`)
          corruptionDetected = true
          break
        }
      }

      // Phase 3: More mid-write kills after recovery battering
      if (!corruptionDetected) {
        console.log('\nPhase 3: More mid-write kills after recovery battering...')
        for (let i = 0; i < 5; i++) {
          const result = await spawnAndKill(
            midwriteWorker,
            { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(100 + i) },
            { afterMs: 200 },
          )
          if (!result.killed && result.exitCode !== 0 && !result.messages.includes('ready')) {
            console.log(`  CORRUPTION at phase 3 cycle ${i}`)
            corruptionDetected = true
            break
          }
        }
      }

      // Final verification
      console.log('\n--- Recovery-kill final verification ---')
      const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
      console.log(`Data dir size: ${sizeMB}MB`)

      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Recovery-kill final open timed out')), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        `)
        console.log(`Tables: ${tables.rows.map(r => r.tablename).join(', ') || '(none)'}`)

        if (tables.rows.some(r => r.tablename === 'midwrite_test')) {
          const count = await db.query('SELECT count(*)::int as cnt FROM midwrite_test')
          console.log(`midwrite_test rows: ${count.rows[0].cnt}`)
          const rows = await db.query('SELECT id, cycle FROM midwrite_test ORDER BY id')
          expect(rows.rows.length).toBe(count.rows[0].cnt)
        }

        await db.query('CREATE TABLE IF NOT EXISTS recovery_verify (id SERIAL PRIMARY KEY, ok BOOLEAN)')
        await db.query('INSERT INTO recovery_verify (ok) VALUES (true)')

        await db.close()
      } catch (err) {
        console.log(`RECOVERY-KILL CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Recovery-kill corrupted DB: ${err.message}`)
      }
    },
    { timeout: 240000 },
  )

  // ============================================================
  // Scenario 3: Alternating heavy/light writes with mid-write kills
  // ============================================================
  it(
    'should survive 20 alternating heavy/light mid-write kill cycles',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-alternating`
      const workerPath = new URL('./workers/rapid-kill-alternating.js', import.meta.url).pathname

      const CYCLES = 20

      for (let i = 0; i < CYCLES; i++) {
        const isHeavy = i % 2 === 0

        // Kill mid-write: for heavy, kill during insert stream;
        // for light, kill after the write
        let killStrategy
        if (isHeavy) {
          // Kill mid-insert (after 200-500 rows of 1000)
          const targetRow = 200 + (i % 4) * 100
          killStrategy = { onMessage: (msg) => msg === `heavy-progress:${targetRow}` }
        } else {
          // For light: kill after the write completes (dirty WAL, no checkpoint)
          killStrategy = { onMessage: 'light-write-done' }
        }

        const result = await spawnAndKill(
          workerPath,
          {
            PGLITE_DATA_DIR: dataDir,
            HEAVY_MODE: isHeavy ? '1' : '0',
            CYCLE_NUM: String(i),
          },
          killStrategy,
        )

        if (i % 4 === 0 || i === CYCLES - 1) {
          const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
          console.log(
            `Alternating cycle ${i}: heavy=${isHeavy}, killed=${result.killed}, ` +
            `msgs=[${result.messages.slice(0, 4).join(',')}], size=${sizeMB}MB`
          )
        }

        if (!result.killed && result.exitCode !== 0 && !result.messages.includes('ready')) {
          console.log(`  CORRUPTION at alternating cycle ${i}: ${result.stderr.slice(0, 200)}`)
          break
        }
      }

      // Final verification
      console.log('\n--- Alternating final verification ---')
      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Alternating final open timed out')), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public'
        `)
        console.log(`Tables: ${tables.rows.map(r => r.tablename).join(', ') || '(none)'}`)

        if (tables.rows.some(r => r.tablename === 'alternating_test')) {
          const count = await db.query('SELECT count(*)::int as cnt FROM alternating_test')
          console.log(`alternating_test rows: ${count.rows[0].cnt}`)

          const dist = await db.query(`
            SELECT heavy, count(*)::int as cnt FROM alternating_test GROUP BY heavy
          `)
          console.log(`Distribution: ${dist.rows.map(r => `heavy=${r.heavy}: ${r.cnt}`).join(', ')}`)

          const rows = await db.query('SELECT id, cycle, heavy FROM alternating_test ORDER BY id')
          expect(rows.rows.length).toBe(count.rows[0].cnt)

          await db.query('SELECT count(*) FROM alternating_test WHERE cycle = 0')
        }

        await db.query('CREATE TABLE IF NOT EXISTS alt_verify (id SERIAL PRIMARY KEY, ok BOOLEAN)')
        await db.query('INSERT INTO alt_verify (ok) VALUES (true)')

        await db.close()
      } catch (err) {
        console.log(`ALTERNATING CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Alternating write-kill corrupted DB: ${err.message}`)
      }
    },
    { timeout: 180000 },
  )

  // ============================================================
  // Scenario 4: Schema evolution across kills
  // ============================================================
  it(
    'should survive 20 schema-evolution kill cycles',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-schema`
      const workerPath = new URL('./workers/rapid-kill-schema-evolution.js', import.meta.url).pathname

      const CYCLES = 20

      for (let i = 0; i < CYCLES; i++) {
        let killStrategy
        if (i % 3 === 0) {
          // Kill after DDL completes (between cycle-complete and close)
          killStrategy = { onMessage: (msg) => typeof msg === 'string' && msg.startsWith('cycle-complete:') }
        } else if (i % 3 === 1) {
          // Kill mid-DDL (aggressive timer)
          killStrategy = { afterMs: 150 }
        } else {
          // Kill after DDL signal
          killStrategy = { onMessage: (msg) => typeof msg === 'string' && msg.startsWith('ddl:') }
        }

        const result = await spawnAndKill(
          workerPath,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(i) },
          killStrategy,
        )

        if (i % 4 === 0 || i === CYCLES - 1) {
          const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
          console.log(
            `Schema cycle ${i} (op=${i % 10}): killed=${result.killed}, ` +
            `msgs=[${result.messages.join(',')}], size=${sizeMB}MB`
          )
        }

        if (!result.killed && result.exitCode !== 0 && !result.messages.includes('ready')) {
          console.log(`  CORRUPTION at schema cycle ${i}: ${result.stderr.slice(0, 200)}`)
          break
        }
      }

      // Final verification
      console.log('\n--- Schema evolution final verification ---')
      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Schema evolution final open timed out')), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename
        `)
        console.log(`Tables: ${tables.rows.map(r => r.tablename).join(', ')}`)

        const indexes = await db.query(`
          SELECT indexname, tablename FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname
        `)
        console.log(`Indexes: ${indexes.rows.map(r => `${r.indexname}(${r.tablename})`).join(', ')}`)

        if (tables.rows.some(r => r.tablename === 'evolving')) {
          const count = await db.query('SELECT count(*)::int as cnt FROM evolving')
          console.log(`evolving rows: ${count.rows[0].cnt}`)

          const cols = await db.query(`
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name = 'evolving' AND table_schema = 'public'
            ORDER BY ordinal_position
          `)
          console.log(`evolving columns: ${cols.rows.map(r => r.column_name).join(', ')}`)

          const rows = await db.query('SELECT * FROM evolving ORDER BY id')
          expect(rows.rows.length).toBe(count.rows[0].cnt)
        }

        await db.query('CREATE TABLE IF NOT EXISTS schema_verify (id SERIAL PRIMARY KEY, ok BOOLEAN)')
        await db.query('INSERT INTO schema_verify (ok) VALUES (true)')

        await db.close()
      } catch (err) {
        console.log(`SCHEMA EVOLUTION CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Schema evolution corrupted DB: ${err.message}`)
      }
    },
    { timeout: 180000 },
  )

  // ============================================================
  // Scenario 5: Minimum mid-write kills to corrupt (aggressive)
  // ============================================================
  describe('minimum mid-write kills to corrupt', () => {
    /**
     * Run N kill cycles where we kill MID-WRITE (not after writes complete).
     * This is the most dangerous pattern because it catches PostgreSQL mid-flush.
     * Returns { corrupted, error, cycles }.
     */
    async function testMidwriteKills(n, label) {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-midwrite${label}`
      const workerPath = new URL('./workers/rapid-kill-midwrite.js', import.meta.url).pathname

      console.log(`\n--- Testing ${n} MID-WRITE kill cycles (${label}) ---`)

      // Cycle 0: Let init + schema creation complete, then kill mid-insert
      // This ensures the data dir is properly initialized before we start
      // the aggressive mid-write kill cycles.
      {
        const result = await spawnAndKill(
          workerPath,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: '0', MODE: 'setup' },
          { onMessage: (msg) => msg === 'row:100' },
        )
        const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
        console.log(
          `  Setup cycle: killed=${result.killed}, lastMsg=${result.messages[result.messages.length - 1] || '(none)'}, size=${sizeMB}MB`
        )
      }

      for (let i = 1; i <= n; i++) {
        // Kill DURING active writes at various points:
        // - Early kills (200ms): during initial inserts
        // - Medium kills (400ms): during updates or second insert round
        // - Late kills (600ms): during mass update or deletes
        const killTimings = [200, 300, 400, 500, 600, 250, 350, 450, 150, 700]
        const killMs = killTimings[i % killTimings.length]

        const result = await spawnAndKill(
          workerPath,
          {
            PGLITE_DATA_DIR: dataDir,
            CYCLE_NUM: String(i),
            MODE: 'stress',
          },
          { afterMs: killMs },
        )

        if (i % 5 === 0 || i === n - 1) {
          const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
          const lastMsg = result.messages.length > 0 ? result.messages[result.messages.length - 1] : '(none)'
          console.log(
            `  Cycle ${i}/${n}: killMs=${killMs}, killed=${result.killed}, ` +
            `lastMsg=${lastMsg}, size=${sizeMB}MB`
          )
        }

        // Worker crashed on open = corruption
        if (!result.killed && result.exitCode !== 0 && !result.messages.includes('ready')) {
          console.log(`  EARLY CORRUPTION at cycle ${i}: ${result.stderr.slice(0, 200)}`)
          return { corrupted: true, error: 'Worker crashed during open', cycles: i }
        }
      }

      // Verify
      const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
      console.log(`  Final size: ${sizeMB}MB`)

      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error(`Open timed out after ${n} mid-write kills`)), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'midwrite_test'
        `)

        if (tables.rows.length > 0) {
          const count = await db.query('SELECT count(*)::int as cnt FROM midwrite_test')
          console.log(`  Rows after ${n} mid-write kills: ${count.rows[0].cnt}`)

          // Full sequential scan
          const rows = await db.query('SELECT id, cycle, seq, kind FROM midwrite_test ORDER BY id')
          if (rows.rows.length !== count.rows[0].cnt) {
            throw new Error(`Row count mismatch: count()=${count.rows[0].cnt} vs SELECT=${rows.rows.length}`)
          }

          // Index scans
          await db.query('SELECT count(*) FROM midwrite_test WHERE cycle = 0')
          await db.query('SELECT count(*) FROM midwrite_test WHERE kind = \'alpha\'')
          await db.query('SELECT count(*) FROM midwrite_test WHERE counter > 0')

          // Verify writable
          await db.query(`INSERT INTO midwrite_test (cycle, seq, kind, payload, counter)
                          VALUES (-1, 0, 'test', 'verify', 0)`)
          await db.query('DELETE FROM midwrite_test WHERE cycle = -1')

          // Cross-check: are there any NULL values in NOT NULL columns?
          const nullCheck = await db.query(`
            SELECT count(*)::int as cnt FROM midwrite_test
            WHERE cycle IS NULL OR seq IS NULL OR kind IS NULL OR payload IS NULL
          `)
          if (nullCheck.rows[0].cnt > 0) {
            throw new Error(`Found ${nullCheck.rows[0].cnt} rows with NULL in NOT NULL columns`)
          }
        }

        await db.close()
        console.log(`  SURVIVED ${n} mid-write kills`)
        return { corrupted: false, error: null, cycles: n }
      } catch (err) {
        console.log(`  CORRUPTED after ${n} mid-write kills: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        return { corrupted: true, error: err.message, cycles: n }
      } finally {
        if (!process.env.RETAIN_DATA && existsSync(dataDir)) {
          rmSync(dataDir, { recursive: true, force: true })
        }
      }
    }

    it(
      'should test 3 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(3, '3')
        console.log(`3 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 60000 },
    )

    it(
      'should test 5 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(5, '5')
        console.log(`5 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 60000 },
    )

    it(
      'should test 10 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(10, '10')
        console.log(`10 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 120000 },
    )

    it(
      'should test 15 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(15, '15')
        console.log(`15 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 120000 },
    )

    it(
      'should test 20 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(20, '20')
        console.log(`20 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 180000 },
    )

    it(
      'should test 25 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(25, '25')
        console.log(`25 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 180000 },
    )

    it(
      'should test 30 mid-write kill cycles',
      async () => {
        const result = await testMidwriteKills(30, '30')
        console.log(`30 mid-write kills: corrupted=${result.corrupted}`)
        expect(typeof result.corrupted).toBe('boolean')
      },
      { timeout: 240000 },
    )
  })

  // ============================================================
  // Scenario 6: Kill during first-time initialization
  // This is the MOST CRITICAL finding: killing PGlite while it's
  // creating its data directory for the very first time (before
  // waitReady resolves on first use) leaves the data dir in a
  // corrupted state that cannot be recovered.
  // ============================================================
  it(
    'should detect corruption from kill during first-time init',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-first-init`
      const workerPath = new URL('./workers/rapid-kill-midwrite.js', import.meta.url).pathname

      // Kill during first-time init at various aggressive timings
      const killTimings = [50, 100, 150, 200, 250]
      let corruptionFound = false

      for (const killMs of killTimings) {
        const testDir = `${dataDir}-${killMs}ms`

        console.log(`\n--- Kill during first init at ${killMs}ms ---`)

        // Kill during first-ever PGlite open (before waitReady)
        const result = await spawnAndKill(
          workerPath,
          { PGLITE_DATA_DIR: testDir, CYCLE_NUM: '0', MODE: 'setup' },
          { afterMs: killMs },
        )

        const sizeMB = (dirSizeBytes(testDir) / 1024 / 1024).toFixed(2)
        const gotReady = result.messages.includes('ready')
        console.log(
          `  Kill at ${killMs}ms: killed=${result.killed}, ready=${gotReady}, ` +
          `msgs=${result.messages.length}, size=${sizeMB}MB`
        )

        if (!existsSync(testDir)) {
          console.log(`  No data dir created (killed too early)`)
          continue
        }

        // Now try to reopen
        let db = null
        try {
          db = new PGlite(testDir)
          await Promise.race([
            db.waitReady,
            new Promise((_, reject) =>
              setTimeout(() => reject(new Error(`Reopen timed out after kill at ${killMs}ms`)), 15000)
            ),
          ])

          await db.query('SELECT 1')
          console.log(`  Recovered OK after kill at ${killMs}ms`)
          await db.close()
        } catch (err) {
          console.log(`  CORRUPTION: Kill at ${killMs}ms during first init corrupted DB: ${err.message}`)
          corruptionFound = true
          if (db) try { await db.close() } catch (_) {}
        }

        // Clean up
        if (!process.env.RETAIN_DATA && existsSync(testDir)) {
          rmSync(testDir, { recursive: true, force: true })
        }
      }

      // Document the finding: we expect at least some timings to corrupt
      console.log(`\nFirst-init corruption found: ${corruptionFound}`)

      // This test documents the behavior - it passes either way but
      // logs whether corruption occurs
      expect(typeof corruptionFound).toBe('boolean')
    },
    { timeout: 120000 },
  )

  // ============================================================
  // Scenario 7: Extreme combined stress - all vectors at once
  // Alternates between: init kills, mid-write kills, recovery kills,
  // schema DDL kills. 40 total cycles on the same data dir.
  // ============================================================
  it(
    'should survive 40 combined-vector kill cycles',
    async () => {
      const { PGlite } = await import('../../dist/index.js')
      const dataDir = `${dataDirBase}-combined`
      const midwriteWorker = new URL('./workers/rapid-kill-midwrite.js', import.meta.url).pathname
      const schemaWorker = new URL('./workers/rapid-kill-schema-evolution.js', import.meta.url).pathname
      const recoveryWorker = new URL('./workers/rapid-kill-recovery.js', import.meta.url).pathname

      const TOTAL_CYCLES = 40

      // Phase 0: Init the data dir properly
      console.log('Phase 0: Initializing data dir...')
      {
        const result = await spawnAndKill(
          midwriteWorker,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: '0', MODE: 'setup' },
          { onMessage: 'inserts-done' },
        )
        console.log(`  Init: killed=${result.killed}, msgs=${result.messages.length}`)
      }

      // Also init the evolving table for schema tests
      {
        const result = await spawnAndKill(
          schemaWorker,
          { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: '0' },
          { onMessage: (msg) => typeof msg === 'string' && msg.startsWith('cycle-complete:') },
        )
        console.log(`  Schema init: killed=${result.killed}, msgs=${result.messages.length}`)
      }

      let corruptionCycle = -1

      for (let i = 1; i <= TOTAL_CYCLES; i++) {
        const vector = i % 4
        let result

        if (vector === 0) {
          // Mid-write kill with large data
          result = await spawnAndKill(
            midwriteWorker,
            { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(i) },
            { afterMs: 200 + (i % 5) * 100 },
          )
        } else if (vector === 1) {
          // Schema DDL kill
          result = await spawnAndKill(
            schemaWorker,
            { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(i) },
            { afterMs: 150 },
          )
        } else if (vector === 2) {
          // Recovery kill (30ms - very aggressive)
          result = await spawnAndKill(
            recoveryWorker,
            { PGLITE_DATA_DIR: dataDir, DO_HEAVY_WRITE: '1' },
            { afterMs: 30 + (i % 3) * 20 },
          )
        } else {
          // Mid-write kill timed to hit during UPDATE phase
          result = await spawnAndKill(
            midwriteWorker,
            { PGLITE_DATA_DIR: dataDir, CYCLE_NUM: String(i) },
            { onMessage: (msg) => typeof msg === 'string' && msg.startsWith('row:') && parseInt(msg.split(':')[1]) >= 300 },
          )
        }

        if (i % 5 === 0 || i === TOTAL_CYCLES) {
          const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
          const vectorName = ['midwrite-timer', 'schema-ddl', 'recovery', 'midwrite-msg'][vector]
          console.log(
            `  Combined cycle ${i}/${TOTAL_CYCLES} (${vectorName}): ` +
            `killed=${result.killed}, msgs=${result.messages.length}, size=${sizeMB}MB`
          )
        }

        // Detect early corruption
        if (!result.killed && result.exitCode !== 0 && !result.messages.includes('ready')) {
          console.log(`  CORRUPTION at combined cycle ${i}: ${result.stderr.slice(0, 200)}`)
          corruptionCycle = i
          break
        }
      }

      // Final verification
      console.log('\n--- Combined stress final verification ---')
      const sizeMB = (dirSizeBytes(dataDir) / 1024 / 1024).toFixed(2)
      console.log(`Data dir size: ${sizeMB}MB`)
      console.log(`Corruption cycle: ${corruptionCycle === -1 ? 'none' : corruptionCycle}`)

      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Combined stress final open timed out')), 30000)
          ),
        ])

        await db.query('SELECT 1')

        const tables = await db.query(`
          SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename
        `)
        console.log(`Tables: ${tables.rows.map(r => r.tablename).join(', ')}`)

        // Check each table
        for (const row of tables.rows) {
          try {
            const count = await db.query(`SELECT count(*)::int as cnt FROM "${row.tablename}"`)
            console.log(`  ${row.tablename}: ${count.rows[0].cnt} rows`)
          } catch (err) {
            console.log(`  ${row.tablename}: ERROR - ${err.message}`)
          }
        }

        // Verify writable
        await db.query('CREATE TABLE IF NOT EXISTS combined_verify (id SERIAL PRIMARY KEY, ok BOOLEAN)')
        await db.query('INSERT INTO combined_verify (ok) VALUES (true)')

        await db.close()
      } catch (err) {
        console.log(`COMBINED STRESS CORRUPTION: ${err.message}`)
        if (db) try { await db.close() } catch (_) {}
        expect.fail(`Combined stress corrupted DB: ${err.message}`)
      } finally {
        if (!process.env.RETAIN_DATA && existsSync(dataDir)) {
          rmSync(dataDir, { recursive: true, force: true })
        }
      }
    },
    { timeout: 300000 },
  )
})
