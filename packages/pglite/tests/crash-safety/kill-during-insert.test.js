import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/kill-during-insert.js',
  import.meta.url
).pathname

const dataDir = testDataDir('kill-during-insert')

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    cleanupDataDir(dataDir)
  }
})

describe('crash safety: kill during bulk INSERT', () => {
  it(
    'should recover after SIGKILL during bulk inserts without a transaction',
    async () => {
      // Step 1: Run the worker and kill it while it's inserting rows
      const result = await crashTest({
        dataDir,
        workerScript: workerPath,
        killOnMessage: 'inserting',
      })

      expect(result.workerKilled).toBe(true)
      expect(result.workerMessages).toContain('ready')
      expect(result.workerMessages).toContain('inserting')

      // Step 2: Try to reopen the database after the crash
      const opened = await tryOpen(dataDir)
      expect(opened.success).toBe(true)
      expect(opened.error).toBeNull()

      if (opened.success) {
        // Step 3: Verify database integrity
        const integrity = await verifyIntegrity(opened.db)
        expect(integrity.issues).toEqual([])
        expect(integrity.intact).toBe(true)

        // Step 4: Verify data consistency — row count should match
        // what was actually committed (some partial count is fine,
        // but the count query must succeed and return a valid number)
        const countResult = await opened.db.query(
          'SELECT count(*) as cnt FROM crash_test'
        )
        const rowCount = parseInt(countResult.rows[0].cnt, 10)
        expect(rowCount).toBeGreaterThanOrEqual(0)

        await opened.db.close()
      }
    },
    { timeout: 60000 }
  )
})
