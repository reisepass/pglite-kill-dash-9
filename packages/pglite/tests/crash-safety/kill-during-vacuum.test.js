import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('kill-during-vacuum')

describe('crash: kill during VACUUM', { timeout: 60000 }, () => {
  afterAll(() => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it('should recover after being killed during VACUUM FULL', async () => {
    // Step 1: Run the worker and kill it while vacuuming
    const result = await crashTest({
      dataDir,
      workerScript: new URL('./workers/kill-during-vacuum.js', import.meta.url)
        .pathname,
      killOnMessage: 'vacuuming',
    })

    expect(result.workerKilled).toBe(true)

    // Step 2: Try to reopen the database
    const { success, db, error } = await tryOpen(dataDir)
    expect(success).toBe(true)
    expect(error).toBeNull()

    try {
      // Step 3: Verify general integrity
      const integrity = await verifyIntegrity(db)
      expect(integrity.intact).toBe(true)

      // Step 4: Verify surviving rows are accessible and correct
      // We inserted 500 rows (id 1-500), deleted even ids, so odd ids survive
      const countResult = await db.query(
        'SELECT count(*)::int AS cnt FROM vacuum_test'
      )
      const count = countResult.rows[0].cnt

      // Regardless of whether VACUUM completed, the surviving rows
      // (odd ids) should be present
      expect(count).toBe(250)

      // Verify the actual data is correct
      const rows = await db.query(
        'SELECT id, value FROM vacuum_test ORDER BY id LIMIT 5'
      )
      expect(rows.rows.length).toBeGreaterThan(0)
      // First surviving row should be id=1
      expect(rows.rows[0].id).toBe(1)
      expect(rows.rows[0].value).toBe('row-0')
    } finally {
      await db.close()
    }
  })
})
