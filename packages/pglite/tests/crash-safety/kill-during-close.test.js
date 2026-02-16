import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('kill-during-close')

describe('crash: kill during close() checkpoint', { timeout: 60000 }, () => {
  afterAll(() => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it('should recover after being killed during close() WAL checkpoint', async () => {
    // Step 1: Run the worker and kill it as soon as it starts closing
    // The 'closing' message fires right before db.close(), which triggers
    // ShutdownXLOG -> CreateCheckPoint -> CheckPointBuffers
    const result = await crashTest({
      dataDir,
      workerScript: new URL('./workers/kill-during-close.js', import.meta.url)
        .pathname,
      killOnMessage: 'closing',
    })

    expect(result.workerKilled).toBe(true)
    // 'closed' should NOT be in messages — we killed during close
    expect(result.workerMessages).toContain('loaded')
    expect(result.workerMessages).toContain('closing')

    // Step 2: Try to reopen the database
    const { success, db, error } = await tryOpen(dataDir)
    expect(success).toBe(true)
    expect(error).toBeNull()

    try {
      // Step 3: Verify general integrity
      const integrity = await verifyIntegrity(db)
      expect(integrity.intact).toBe(true)

      // Step 4: Verify data survived — we inserted 500 rows
      const countResult = await db.query(
        'SELECT count(*)::int AS cnt FROM close_test'
      )
      const count = countResult.rows[0].cnt
      expect(count).toBe(500)

      // Step 5: Verify the updates were applied (ids divisible by 3)
      const updatedRows = await db.query(
        `SELECT count(*)::int AS cnt FROM close_test WHERE value LIKE 'updated-%'`
      )
      // ids 3,6,9,...,498 => floor(500/3) = 166 rows
      expect(updatedRows.rows[0].cnt).toBe(166)

      // Step 6: Verify the index exists and is usable
      const indexResult = await db.query(`
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'close_test' AND indexname = 'idx_close_test_value'
      `)
      expect(indexResult.rows.length).toBe(1)

      // Step 7: Verify we can still write to the database
      await db.query(
        `INSERT INTO close_test (value, padding) VALUES ('post-crash', 'ok')`
      )
      const postCrash = await db.query(
        `SELECT count(*)::int AS cnt FROM close_test`
      )
      expect(postCrash.rows[0].cnt).toBe(501)
    } finally {
      await db.close()
    }
  })
})
