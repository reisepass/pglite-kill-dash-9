import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('kill-during-index')

describe('crash during CREATE INDEX', () => {
  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it(
    'should recover after being killed during index creation',
    async () => {
      // Phase 1: Run the worker and kill it when it starts creating the index
      const result = await crashTest({
        dataDir,
        workerScript: new URL(
          './workers/kill-during-index-creation.js',
          import.meta.url
        ).pathname,
        killOnMessage: 'creating-index',
      })

      expect(result.workerKilled).toBe(true)
      // The worker should have been killed before index-done
      // (it may or may not have sent it depending on timing)

      // Phase 2: Try to reopen the database
      const { success, db, error } = await tryOpen(dataDir)
      expect(success).toBe(true)
      expect(error).toBeNull()

      try {
        // Phase 3: Verify overall database integrity
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        // Phase 4: Verify the table data is still accessible
        const countResult = await db.query(
          'SELECT count(*) AS cnt FROM indexed_data'
        )
        const rowCount = parseInt(countResult.rows[0].cnt, 10)
        // Data was inserted before the index creation signal, so rows should exist
        expect(rowCount).toBeGreaterThan(0)

        // Phase 5: Check whether the index exists -- it's OK if it doesn't,
        // but the table must be intact regardless
        const indexResult = await db.query(`
          SELECT indexname FROM pg_indexes
          WHERE schemaname = 'public'
            AND tablename = 'indexed_data'
            AND indexname = 'idx_indexed_data_value'
        `)

        if (indexResult.rows.length > 0) {
          // Index survived -- verify it works by running a query that could use it
          const probeResult = await db.query(
            'SELECT count(*) AS cnt FROM indexed_data WHERE value = 1'
          )
          expect(parseInt(probeResult.rows[0].cnt, 10)).toBeGreaterThanOrEqual(
            0
          )
        }
        // If the index doesn't exist, that's acceptable after a crash

        // Phase 6: Verify we can still write to the table
        await db.query(
          "INSERT INTO indexed_data (value, payload) VALUES (99999, 'post-crash')"
        )
        const afterInsert = await db.query(
          'SELECT count(*) AS cnt FROM indexed_data'
        )
        expect(parseInt(afterInsert.rows[0].cnt, 10)).toBe(rowCount + 1)
      } finally {
        await db.close()
      }
    },
    { timeout: 60_000 }
  )
})
