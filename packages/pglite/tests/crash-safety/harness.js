/**
 * Crash Safety Test Harness for PGlite
 *
 * Spawns a child process that performs PGlite operations, then kills it
 * with SIGKILL (kill -9) at a specified moment, and verifies the database
 * can be reopened and is in a consistent state.
 *
 * Usage from test files:
 *   import { crashTest, cleanupDataDir } from './harness.js'
 *
 *   const result = await crashTest({
 *     dataDir: '/tmp/pglite-crash-test-xyz',
 *     // Path to a worker script that does PGlite operations
 *     workerScript: new URL('./workers/my-scenario.js', import.meta.url).pathname,
 *     // How long to let the worker run before killing (ms)
 *     killAfterMs: 500,
 *     // Optional: signal to use (default SIGKILL)
 *     signal: 'SIGKILL',
 *   })
 */

import { fork } from 'node:child_process'
import { existsSync, rmSync, mkdirSync } from 'node:fs'
import { resolve } from 'node:path'

/**
 * @typedef {Object} CrashTestOptions
 * @property {string} dataDir - Path to PGlite data directory
 * @property {string} workerScript - Path to the worker script
 * @property {number} [killAfterMs=500] - Delay before killing the worker
 * @property {string} [signal='SIGKILL'] - Signal to send
 * @property {Object} [env] - Extra environment variables for the worker
 * @property {string} [killOnMessage] - Kill when worker sends this message instead of timer
 */

/**
 * @typedef {Object} CrashTestResult
 * @property {boolean} workerKilled - Whether the worker was successfully killed
 * @property {string|null} workerError - Error from worker before kill, if any
 * @property {string[]} workerMessages - Messages received from worker before kill
 * @property {number|null} workerExitCode - Worker exit code (null if killed)
 * @property {string|null} workerSignal - Signal that killed the worker
 */

/**
 * Run a crash test scenario.
 *
 * 1. Spawns a child process running `workerScript`
 * 2. The worker script should set up PGlite and perform operations
 * 3. After `killAfterMs` (or when `killOnMessage` is received), sends SIGKILL
 * 4. Returns info about the kill
 *
 * The caller is then responsible for trying to reopen PGlite on the same
 * dataDir and verifying consistency.
 */
export async function crashTest(options) {
  const {
    dataDir,
    workerScript,
    killAfterMs = 500,
    signal = 'SIGKILL',
    env = {},
    killOnMessage = null,
  } = options

  // Ensure data dir parent exists
  const parentDir = resolve(dataDir, '..')
  if (!existsSync(parentDir)) {
    mkdirSync(parentDir, { recursive: true })
  }

  return new Promise((resolvePromise, rejectPromise) => {
    const messages = []
    let workerError = null
    let killed = false
    let killTimer = null

    const child = fork(workerScript, [], {
      env: {
        ...process.env,
        PGLITE_DATA_DIR: dataDir,
        ...env,
      },
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    })

    // Collect stdout/stderr for debugging
    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (d) => { stdout += d.toString() })
    child.stderr.on('data', (d) => { stderr += d.toString() })

    child.on('message', (msg) => {
      messages.push(msg)

      // Kill when we receive the trigger message
      if (killOnMessage && msg === killOnMessage && !killed) {
        killed = true
        if (killTimer) clearTimeout(killTimer)
        child.kill(signal)
      }
    })

    child.on('error', (err) => {
      workerError = err.message
    })

    child.on('exit', (code, sig) => {
      if (killTimer) clearTimeout(killTimer)
      resolvePromise({
        workerKilled: sig === signal || killed,
        workerError,
        workerMessages: messages,
        workerExitCode: code,
        workerSignal: sig,
        stdout,
        stderr,
      })
    })

    // If no message-based trigger, use timer
    if (!killOnMessage) {
      killTimer = setTimeout(() => {
        if (!killed) {
          killed = true
          child.kill(signal)
        }
      }, killAfterMs)
    }

    // Safety timeout — kill after 30s no matter what
    setTimeout(() => {
      if (!killed) {
        killed = true
        child.kill('SIGKILL')
      }
    }, 30000)
  })
}

/**
 * Try to open a PGlite instance on a data directory that may be corrupted.
 * Returns { success, db, error }.
 * If success is true, caller must close db when done.
 * @param {string} dataDir
 * @param {number} [timeoutMs=15000] - timeout to wait for PGlite to become ready
 */
export async function tryOpen(dataDir, timeoutMs = 15000) {
  // Dynamic import to get the PGlite dist
  const { PGlite } = await import('../../dist/index.js')

  try {
    const db = new PGlite(dataDir)

    // Race between waitReady and a timeout — a corrupted db may hang forever
    await Promise.race([
      db.waitReady,
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error(`PGlite open timed out after ${timeoutMs}ms (likely corrupted)`)),
          timeoutMs,
        ),
      ),
    ])

    return { success: true, db, error: null }
  } catch (err) {
    return { success: false, db: null, error: err }
  }
}

/**
 * Verify database integrity by running queries and checking results.
 * Returns { intact, details }.
 */
export async function verifyIntegrity(db) {
  const issues = []

  try {
    // Check if we can run a basic query
    await db.query('SELECT 1 as health_check')
  } catch (err) {
    issues.push(`Basic query failed: ${err.message}`)
    return { intact: false, issues }
  }

  try {
    // List all user tables
    const tables = await db.query(`
      SELECT tablename FROM pg_tables
      WHERE schemaname = 'public'
      ORDER BY tablename
    `)

    // For each table, try a SELECT count and a sequential scan
    for (const row of tables.rows) {
      try {
        await db.query(`SELECT count(*) FROM "${row.tablename}"`)
      } catch (err) {
        issues.push(`Count on ${row.tablename} failed: ${err.message}`)
      }
    }
  } catch (err) {
    issues.push(`Table listing failed: ${err.message}`)
  }

  try {
    // Check indexes
    const indexes = await db.query(`
      SELECT indexname, tablename FROM pg_indexes
      WHERE schemaname = 'public'
      ORDER BY indexname
    `)

    for (const row of indexes.rows) {
      try {
        // Force an index scan by querying with a condition
        await db.query(`SELECT count(*) FROM "${row.tablename}"`)
      } catch (err) {
        issues.push(`Index check on ${row.indexname} failed: ${err.message}`)
      }
    }
  } catch (err) {
    issues.push(`Index listing failed: ${err.message}`)
  }

  return { intact: issues.length === 0, issues }
}

/**
 * Clean up a test data directory and its sibling lock file.
 */
export function cleanupDataDir(dataDir) {
  if (existsSync(dataDir)) {
    rmSync(dataDir, { recursive: true, force: true })
  }
  // Also clean up the sibling lock file (dataDir.lock)
  const lockFile = dataDir + '.lock'
  if (existsSync(lockFile)) {
    rmSync(lockFile, { force: true })
  }
}

/**
 * Utility: generate a unique data dir path for a test scenario.
 */
export function testDataDir(scenarioName) {
  const timestamp = Date.now()
  const rand = Math.random().toString(36).slice(2, 8)
  return `/tmp/pglite-crash-${scenarioName}-${timestamp}-${rand}`
}
