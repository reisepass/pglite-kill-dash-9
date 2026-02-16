import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import { existsSync, rmSync, mkdirSync } from 'node:fs'

const dataDir = `/tmp/pglite-crash-double-open-${Date.now()}`

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    if (existsSync(dataDir)) {
      rmSync(dataDir, { recursive: true, force: true })
    }
  }
})

describe('crash safety: concurrent double-open corruption', () => {
  it(
    'should prevent or survive two instances opening the same dataDir',
    async () => {
      const { PGlite } = await import('../../dist/index.js')

      // Step 1: Create a fresh database with the table
      const setup = new PGlite(dataDir)
      await setup.waitReady

      await setup.query(`
        CREATE TABLE IF NOT EXISTS double_open_test (
          id SERIAL PRIMARY KEY,
          instance TEXT NOT NULL,
          value TEXT NOT NULL,
          created_at TIMESTAMP DEFAULT NOW()
        )
      `)
      await setup.close()

      // Step 2: Spawn TWO child processes on the SAME dataDir simultaneously
      const workerPath = new URL(
        './workers/double-open-worker.js',
        import.meta.url,
      ).pathname

      function spawnWorker(instanceId, killAfterMs = 3000) {
        return new Promise((resolve) => {
          const messages = []
          let killed = false

          const child = fork(workerPath, [], {
            env: {
              ...process.env,
              PGLITE_DATA_DIR: dataDir,
              INSTANCE_ID: instanceId,
            },
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
          })

          let stderr = ''
          child.stderr.on('data', (d) => { stderr += d.toString() })

          child.on('message', (msg) => { messages.push(msg) })

          child.on('exit', (code, sig) => {
            resolve({ killed: killed || sig === 'SIGKILL', messages, exitCode: code, signal: sig, stderr })
          })

          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill('SIGKILL')
            }
          }, killAfterMs)

          // Safety
          setTimeout(() => {
            if (!killed) { killed = true; child.kill('SIGKILL') }
          }, 30000)
        })
      }

      const [resultA, resultB] = await Promise.all([
        spawnWorker('A', 3000),
        spawnWorker('B', 3000),
      ])

      // At least one should have started running
      const aRan = resultA.messages.includes('running') || resultA.messages.includes('table_created')
      const bRan = resultB.messages.includes('running')
      expect(aRan || bRan).toBe(true)

      // Step 3: Try to reopen the database after both are killed
      let openSuccess = false
      let db = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('Timed out opening corrupted DB')), 15000),
          ),
        ])
        openSuccess = true
      } catch (err) {
        // Database is corrupted — this proves the problem
        openSuccess = false
      }

      // The FUTURE expectation: PGlite should either lock or recover
      expect(openSuccess).toBe(true)

      if (openSuccess && db) {
        const countResult = await db.query(
          'SELECT count(*) as cnt FROM double_open_test',
        )
        const rowCount = parseInt(countResult.rows[0].cnt, 10)
        expect(rowCount).toBeGreaterThanOrEqual(0)
        await db.close()
      }
    },
    { timeout: 60000 },
  )
})
