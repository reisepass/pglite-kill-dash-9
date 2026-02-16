import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('batch-load')

describe('Crash safety: kill during large batch load', () => {
  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it(
    'should recover after SIGKILL during a large batch INSERT',
    async () => {
      // Run the worker and kill it when it signals the big batch is starting
      const result = await crashTest({
        dataDir,
        workerScript: new URL(
          './workers/kill-during-batch-load.js',
          import.meta.url
        ).pathname,
        killOnMessage: 'loading',
      })

      expect(result.workerKilled).toBe(true)

      // Reopen the database after the crash
      const { success, db, error } = await tryOpen(dataDir)
      expect(success).toBe(true)
      expect(error).toBeNull()

      try {
        // Verify general integrity
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        // Baseline rows (first 10) should be present since they were synced
        const baseline = await db.query(
          `SELECT count(*)::int as cnt FROM batch_test WHERE name LIKE 'baseline-%'`
        )
        expect(baseline.rows[0].cnt).toBe(10)

        // The total count must be atomic: either only baseline (10) or
        // baseline + full batch (5010). It must NOT be a partial batch.
        const total = await db.query(
          `SELECT count(*)::int as cnt FROM batch_test`
        )
        const count = total.rows[0].cnt
        expect(
          count === 10 || count === 5010,
          `Expected 10 (batch rolled back) or 5010 (batch committed), but got ${count}`
        ).toBe(true)
      } finally {
        await db.close()
      }
    },
    { timeout: 60000 }
  )
})
