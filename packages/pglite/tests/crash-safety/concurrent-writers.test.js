import { describe, it, expect, afterAll } from 'vitest'
import { fork } from 'node:child_process'
import { existsSync, rmSync } from 'node:fs'

const dataDir = `/tmp/pglite-crash-concurrent-writers-${Date.now()}`

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    if (existsSync(dataDir)) {
      rmSync(dataDir, { recursive: true, force: true })
    }
  }
})

describe('crash safety: concurrent writers corruption', () => {
  it(
    'should detect corruption when two processes write to the same dataDir simultaneously',
    async () => {
      const { PGlite } = await import('../../dist/index.js')

      // Step 1: Create a fresh database with the table, then close it
      const setup = new PGlite(dataDir)
      await setup.waitReady

      await setup.query(`
        CREATE TABLE IF NOT EXISTS concurrent_test (
          id SERIAL PRIMARY KEY,
          writer TEXT NOT NULL,
          data TEXT NOT NULL,
          seq INT NOT NULL
        )
      `)
      await setup.close()

      // Step 2: Spawn TWO child processes simultaneously on the SAME dataDir
      const workerPath = new URL(
        './workers/concurrent-writers.js',
        import.meta.url,
      ).pathname

      function spawnWriter(writerId, killAfterMs = 5000) {
        return new Promise((resolve) => {
          const messages = []
          let killed = false

          const child = fork(workerPath, [], {
            env: {
              ...process.env,
              PGLITE_DATA_DIR: dataDir,
              WRITER_ID: writerId,
            },
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
          })

          let stderr = ''
          child.stderr.on('data', (d) => {
            stderr += d.toString()
          })

          child.on('message', (msg) => {
            messages.push(msg)
          })

          child.on('exit', (code, sig) => {
            resolve({
              writerId,
              killed: killed || sig === 'SIGKILL',
              messages,
              exitCode: code,
              signal: sig,
              stderr,
            })
          })

          // Kill after the specified duration
          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill('SIGKILL')
            }
          }, killAfterMs)

          // Safety timeout
          setTimeout(() => {
            if (!killed) {
              killed = true
              child.kill('SIGKILL')
            }
          }, 30000)
        })
      }

      // Spawn both writers simultaneously — they'll both write for 5 seconds
      const [result1, result2] = await Promise.all([
        spawnWriter('writer-1', 5000),
        spawnWriter('writer-2', 5000),
      ])

      // At least one writer should have started
      const writer1Ran =
        result1.messages.includes('ready') ||
        result1.messages.includes('table_ready')
      const writer2Ran =
        result2.messages.includes('ready') ||
        result2.messages.includes('table_ready')
      expect(writer1Ran || writer2Ran).toBe(true)

      // Wait 500ms for filesystem to settle
      await new Promise((r) => setTimeout(r, 500))

      // Step 3: Try to reopen the database after both are killed
      let openSuccess = false
      let db = null
      let openError = null
      try {
        db = new PGlite(dataDir)
        await Promise.race([
          db.waitReady,
          new Promise((_, reject) =>
            setTimeout(
              () =>
                reject(
                  new Error('Timed out opening DB after concurrent writes'),
                ),
              15000,
            ),
          ),
        ])
        openSuccess = true
      } catch (err) {
        openSuccess = false
        openError = err
      }

      // Step 4: Check consistency
      // The FUTURE expectation (when file locking is implemented):
      //   PGlite should either lock the dataDir (preventing the second process)
      //   or recover cleanly.
      // For now, we expect EITHER:
      //   a) The DB fails to open (corruption detected) — expected failure
      //   b) The DB opens, and we verify data consistency
      expect(openSuccess).toBe(true)

      if (openSuccess && db) {
        // Verify data makes sense
        const countResult = await db.query(
          'SELECT count(*) as cnt FROM concurrent_test',
        )
        const totalRows = parseInt(countResult.rows[0].cnt, 10)
        expect(totalRows).toBeGreaterThanOrEqual(0)

        // Check that rows from both writers exist (if any were inserted)
        const writerStats = await db.query(`
          SELECT writer, count(*) as cnt, min(seq) as min_seq, max(seq) as max_seq
          FROM concurrent_test
          GROUP BY writer
          ORDER BY writer
        `)

        // Validate data integrity: no garbage writer IDs
        for (const row of writerStats.rows) {
          expect(['writer-1', 'writer-2']).toContain(row.writer)
          const cnt = parseInt(row.cnt, 10)
          expect(cnt).toBeGreaterThan(0)
          // Sequences should be non-negative
          expect(parseInt(row.min_seq, 10)).toBeGreaterThanOrEqual(0)
        }

        // Check for duplicate sequences per writer (would indicate corruption)
        const dupes = await db.query(`
          SELECT writer, seq, count(*) as cnt
          FROM concurrent_test
          GROUP BY writer, seq
          HAVING count(*) > 1
        `)
        // Duplicates would be a sign of corruption
        expect(dupes.rows.length).toBe(0)

        await db.close()
      } else {
        // DB failed to open — this IS the expected failure mode
        // Document the corruption error for visibility
        console.log(
          'Database failed to open after concurrent writes (expected):',
          openError?.message,
        )
      }
    },
    { timeout: 60000 },
  )
})
