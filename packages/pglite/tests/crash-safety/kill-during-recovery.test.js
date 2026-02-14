import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/kill-during-recovery.js',
  import.meta.url
).pathname

const dataDir = testDataDir('kill-during-recovery')

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    cleanupDataDir(dataDir)
  }
})

describe('crash safety: kill during WAL recovery (double-crash)', () => {
  it(
    'should survive a crash during WAL recovery and open on the third attempt',
    async () => {
      // Phase 1: Create data and SIGKILL while inserting (leaves dirty WAL)
      const phase1 = await crashTest({
        dataDir,
        workerScript: workerPath,
        killOnMessage: 'inserting',
        env: { PHASE: '1' },
      })

      expect(phase1.workerKilled).toBe(true)
      expect(phase1.workerMessages).toContain('ready')
      expect(phase1.workerMessages).toContain('inserting')

      // Phase 2: Open the dirty datadir (triggers WAL recovery), then
      // SIGKILL after 150ms — during recovery before it can finish
      const phase2 = await crashTest({
        dataDir,
        workerScript: workerPath,
        killAfterMs: 150,
        env: { PHASE: '2' },
      })

      expect(phase2.workerKilled).toBe(true)
      // The worker should have at least sent 'opening' before we killed it
      expect(phase2.workerMessages).toContain('opening')

      // Phase 3: Try to open the database a third time.
      // This is the critical test — WAL recovery was itself interrupted.
      const opened = await tryOpen(dataDir)
      expect(opened.success).toBe(true)
      expect(opened.error).toBeNull()

      if (opened.success) {
        // Verify structural integrity
        const integrity = await verifyIntegrity(opened.db)
        expect(integrity.issues).toEqual([])
        expect(integrity.intact).toBe(true)

        // The table should exist (it was created and committed in phase 1)
        const tableCheck = await opened.db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename = 'recovery_test'
        `)
        expect(tableCheck.rows.length).toBe(1)

        // Row count is uncertain (phase 1 was killed mid-insert) but must
        // be a valid number >= 0 and the query must succeed
        const countResult = await opened.db.query(
          'SELECT count(*) as cnt FROM recovery_test'
        )
        const rowCount = parseInt(countResult.rows[0].cnt, 10)
        expect(rowCount).toBeGreaterThanOrEqual(0)

        // Verify we can read all rows without errors (sequential scan)
        const rows = await opened.db.query(
          'SELECT id, value FROM recovery_test ORDER BY id'
        )
        expect(rows.rows.length).toBe(rowCount)

        // Verify we can still write new data after the double-crash recovery
        await opened.db.query(
          `INSERT INTO recovery_test (value) VALUES ($1)`,
          ['post-recovery-write']
        )
        const afterInsert = await opened.db.query(
          'SELECT count(*) as cnt FROM recovery_test'
        )
        expect(parseInt(afterInsert.rows[0].cnt, 10)).toBe(rowCount + 1)

        await opened.db.close()
      }
    },
    { timeout: 120000 }
  )
})
