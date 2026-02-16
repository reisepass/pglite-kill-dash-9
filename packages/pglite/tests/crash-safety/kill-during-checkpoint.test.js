import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import {
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/kill-during-checkpoint.js',
  import.meta.url
).pathname

/**
 * Custom crash test that kills the worker after a specified delay
 * from receiving the checkpoint-starting message.
 */
function crashTestWithDelay(options) {
  const {
    dataDir,
    workerScript,
    killDelayMs = 0,
    signal = 'SIGKILL',
    env = {},
  } = options

  return new Promise((resolve) => {
    const messages = []
    let workerError = null
    let killed = false

    const child = fork(workerScript, [], {
      env: {
        ...process.env,
        PGLITE_DATA_DIR: dataDir,
        ...env,
      },
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    })

    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (d) => { stdout += d.toString() })
    child.stderr.on('data', (d) => { stderr += d.toString() })

    child.on('message', (msg) => {
      messages.push(msg)

      if (msg === 'checkpoint-starting' && !killed) {
        if (killDelayMs <= 0) {
          killed = true
          child.kill(signal)
        } else {
          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill(signal)
            }
          }, killDelayMs)
        }
      }
    })

    child.on('error', (err) => {
      workerError = err.message
    })

    child.on('exit', (code, sig) => {
      resolve({
        workerKilled: sig === signal || killed,
        workerError,
        workerMessages: messages,
        workerExitCode: code,
        workerSignal: sig,
        stdout,
        stderr,
        completedCheckpoint: messages.includes('checkpoint-done'),
      })
    })

    // Safety timeout
    setTimeout(() => {
      if (!killed) {
        killed = true
        child.kill('SIGKILL')
      }
    }, 120000)
  })
}

describe('crash safety: kill during CHECKPOINT', { timeout: 600000 }, () => {
  // Focus on very short delays that actually catch checkpoint
  // From first run: 0ms = 5/5 kills, 5ms = 5/5 kills, 10ms = 1/5 kills
  const delays = [0, 1, 2, 3, 5]
  const cyclesPerDelay = 8
  const dataDirs = delays.map((d) => testDataDir(`kill-checkpoint-${d}ms`))

  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      dataDirs.forEach((dir) => cleanupDataDir(dir))
    }
  })

  for (let delayIdx = 0; delayIdx < delays.length; delayIdx++) {
    const delay = delays[delayIdx]
    const dataDir = dataDirs[delayIdx]

    it(
      `should handle kill ${delay}ms after CHECKPOINT starts (${cyclesPerDelay} cycles)`,
      async () => {
        let corruptionFound = false
        let corruptionDetails = []
        let killsDuringCheckpoint = 0
        let totalCyclesRun = 0

        for (let cycle = 0; cycle < cyclesPerDelay; cycle++) {
          totalCyclesRun++

          // Step 1: Run worker - insert heavy data then start checkpoint
          const result = await crashTestWithDelay({
            dataDir,
            workerScript: workerPath,
            killDelayMs: delay,
            env: { CRASH_CYCLE: String(cycle) },
          })

          if (result.completedCheckpoint) {
            console.log(
              `[delay=${delay}ms cycle=${cycle}] Checkpoint completed before kill`
            )
            // Properly close db after completed checkpoint
            const cleanup = await tryOpen(dataDir)
            if (cleanup.success) await cleanup.db.close()
            continue
          }

          if (
            result.workerMessages.includes('checkpoint-starting') &&
            !result.completedCheckpoint
          ) {
            killsDuringCheckpoint++
            console.log(
              `[delay=${delay}ms cycle=${cycle}] Killed during checkpoint!`
            )
          } else if (!result.workerMessages.includes('checkpoint-starting')) {
            // Killed before checkpoint even started
            console.log(
              `[delay=${delay}ms cycle=${cycle}] Killed before checkpoint started (messages: ${result.workerMessages.join(', ')})`
            )
          }

          // Step 2: Try to reopen the database after the crash
          const opened = await tryOpen(dataDir, 30000)

          if (!opened.success) {
            corruptionFound = true
            corruptionDetails.push(
              `[delay=${delay}ms cycle=${cycle}] CORRUPTION: Failed to reopen: ${opened.error?.message}`
            )
            console.log(
              `CORRUPTION FOUND: Cannot reopen after kill ${delay}ms into checkpoint (cycle ${cycle})`
            )
            console.log(`Error: ${opened.error?.message}`)
            break
          }

          try {
            // Step 3: Verify integrity
            const integrity = await verifyIntegrity(opened.db)
            if (!integrity.intact) {
              corruptionFound = true
              corruptionDetails.push(
                `[delay=${delay}ms cycle=${cycle}] CORRUPTION: Integrity check failed: ${integrity.issues.join('; ')}`
              )
              console.log(
                `CORRUPTION FOUND: Integrity issues after kill ${delay}ms into checkpoint (cycle ${cycle})`
              )
              console.log(`Issues: ${integrity.issues.join('\n')}`)
              break
            }

            // Step 4: Cross-check data consistency between tables and metadata
            // Tables may not exist if kill happened before table creation
            const tableCheck = await opened.db.query(
              `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'ckpt_meta'`
            )
            if (tableCheck.rows.length > 0) {
              const metaRows = await opened.db.query(
                `SELECT cycle, table_name, row_count FROM ckpt_meta ORDER BY cycle, table_name`
              )
              for (const meta of metaRows.rows) {
                try {
                  const actual = await opened.db.query(
                    `SELECT count(*)::int AS cnt FROM ${meta.table_name} WHERE cycle = $1`,
                    [meta.cycle]
                  )
                  if (actual.rows[0].cnt !== meta.row_count) {
                    corruptionFound = true
                    corruptionDetails.push(
                      `[delay=${delay}ms cycle=${cycle}] DATA INCONSISTENCY: ${meta.table_name} cycle ${meta.cycle} expected ${meta.row_count} rows but found ${actual.rows[0].cnt}`
                    )
                    console.log(
                      `DATA INCONSISTENCY: ${meta.table_name} cycle ${meta.cycle} expected ${meta.row_count} rows but found ${actual.rows[0].cnt}`
                    )
                  }
                } catch (queryErr) {
                  corruptionFound = true
                  corruptionDetails.push(
                    `[delay=${delay}ms cycle=${cycle}] QUERY ERROR on ${meta.table_name}: ${queryErr.message}`
                  )
                }
              }
            }

            // Steps 5-7 only run if tables exist
            const dataTableCheck = await opened.db.query(
              `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'ckpt_t%'`
            )
            if (dataTableCheck.rows.length > 0) {
              // Step 5: Try post-recovery write operations
              try {
                await opened.db.query('BEGIN')
                for (let t = 0; t < 5; t++) {
                  await opened.db.query(
                    `INSERT INTO ckpt_t${t} (cycle, seq, tag, payload) VALUES ($1, $2, $3, $4)`,
                    [9999, 0, 'recovery_test', 'recovery_write_test']
                  )
                }
                await opened.db.query('COMMIT')
                for (let t = 0; t < 5; t++) {
                  await opened.db.query(
                    `DELETE FROM ckpt_t${t} WHERE cycle = 9999`
                  )
                }
              } catch (writeErr) {
                corruptionFound = true
                corruptionDetails.push(
                  `[delay=${delay}ms cycle=${cycle}] WRITE AFTER RECOVERY FAILED: ${writeErr.message}`
                )
                console.log(
                  `CORRUPTION: Post-recovery write failed: ${writeErr.message}`
                )
              }

              // Step 6: Check index usability
              try {
                for (let t = 0; t < 5; t++) {
                  await opened.db.query(
                    `SELECT count(*)::int FROM ckpt_t${t} WHERE tag LIKE 'tag_%'`
                  )
                  await opened.db.query(
                    `SELECT count(*)::int FROM ckpt_t${t} WHERE cycle = 0`
                  )
                }
              } catch (indexErr) {
                corruptionFound = true
                corruptionDetails.push(
                  `[delay=${delay}ms cycle=${cycle}] INDEX CORRUPTION: ${indexErr.message}`
                )
              }

              // Step 7: Full sequential scan to detect page-level corruption
              try {
                for (let t = 0; t < 5; t++) {
                  await opened.db.query(
                    `SELECT id, cycle, seq, length(payload) AS plen FROM ckpt_t${t} ORDER BY id`
                  )
                }
              } catch (scanErr) {
                corruptionFound = true
                corruptionDetails.push(
                  `[delay=${delay}ms cycle=${cycle}] SCAN CORRUPTION: ${scanErr.message}`
                )
              }
            }
          } finally {
            await opened.db.close()
          }

          if (corruptionFound) break
        }

        console.log(
          `[delay=${delay}ms] Kills during checkpoint: ${killsDuringCheckpoint}/${totalCyclesRun}`
        )

        if (corruptionFound) {
          console.log('=== CORRUPTION DETAILS ===')
          corruptionDetails.forEach((d) => console.log(d))
          // Test fails to prove corruption was found
          expect(corruptionDetails).toEqual([])
        } else {
          console.log(
            `[delay=${delay}ms] PGlite survived all ${totalCyclesRun} cycles without corruption`
          )
        }
      },
      { timeout: 240000 }
    )
  }
})
