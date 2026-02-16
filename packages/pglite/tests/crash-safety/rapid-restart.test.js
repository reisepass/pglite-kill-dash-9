import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/rapid-restart-worker.js',
  import.meta.url
).pathname

const dataDir = testDataDir('rapid-restart')

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    cleanupDataDir(dataDir)
  }
})

describe('crash safety: rapid restart cycle', () => {
  it(
    'should recover after multiple rapid dirty shutdowns',
    async () => {
      const totalCycles = 6

      // Cycle 0: let table creation finish, then kill after 'ready'
      const cycle0 = await crashTest({
        dataDir,
        workerScript: workerPath,
        killOnMessage: 'ready',
        env: { CYCLE_NUMBER: '0' },
      })

      expect(cycle0.workerKilled).toBe(true)
      expect(cycle0.workerMessages).toContain('ready')

      // Cycles 1-4: rapid kill with short timer while inserting
      for (let i = 1; i <= 4; i++) {
        const result = await crashTest({
          dataDir,
          workerScript: workerPath,
          killAfterMs: 300,
          env: { CYCLE_NUMBER: String(i) },
        })

        expect(result.workerKilled).toBe(true)
      }

      // Cycle 5: let it run until ready, then kill
      const cycleFinal = await crashTest({
        dataDir,
        workerScript: workerPath,
        killOnMessage: 'ready',
        env: { CYCLE_NUMBER: '5' },
      })

      expect(cycleFinal.workerKilled).toBe(true)
      expect(cycleFinal.workerMessages).toContain('ready')

      // After all cycles, try to open the database and verify integrity
      const opened = await tryOpen(dataDir)
      expect(opened.success).toBe(true)
      expect(opened.error).toBeNull()

      if (opened.success) {
        // Verify structural integrity
        const integrity = await verifyIntegrity(opened.db)
        expect(integrity.issues).toEqual([])
        expect(integrity.intact).toBe(true)

        // Verify the table exists and is queryable
        const tableCheck = await opened.db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename = 'restart_test'
        `)
        expect(tableCheck.rows.length).toBe(1)

        // Row count is uncertain due to kills, but must be a valid number >= 0
        const countResult = await opened.db.query(
          'SELECT count(*) as cnt FROM restart_test'
        )
        const rowCount = parseInt(countResult.rows[0].cnt, 10)
        expect(rowCount).toBeGreaterThanOrEqual(0)

        // Cycle 0 completed before kill, so at least its initial row should exist
        // (cycle 0 was killed after 'ready', meaning its insert completed)
        expect(rowCount).toBeGreaterThanOrEqual(1)

        // Verify we can read the data without errors
        const rows = await opened.db.query(
          'SELECT cycle, value FROM restart_test ORDER BY id'
        )
        expect(rows.rows.length).toBe(rowCount)

        await opened.db.close()
      }
    },
    { timeout: 120000 }
  )
})
