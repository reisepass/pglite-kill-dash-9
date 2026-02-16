/**
 * Crash safety test: kill PGlite with SIGKILL while inside an explicit
 * transaction that has not been committed.
 *
 * Expected outcome:
 * - The database can be reopened after the crash
 * - Baseline rows (committed before the transaction) are intact
 * - All changes made inside the uncommitted transaction are rolled back
 */
import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('kill-during-txn')

describe('kill during transaction', { timeout: 60_000 }, () => {
  let db

  afterAll(async () => {
    if (db) {
      try {
        await db.close()
      } catch (_) {
        // already closed or broken
      }
    }
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it('should recover after SIGKILL during an uncommitted transaction', async () => {
    // Step 1: Run the worker and kill it while it is inside a transaction
    const result = await crashTest({
      dataDir,
      workerScript: new URL(
        './workers/kill-during-transaction.js',
        import.meta.url,
      ).pathname,
      killOnMessage: 'in-transaction',
    })

    expect(result.workerKilled).toBe(true)
    expect(result.workerMessages).toContain('in-transaction')

    // Step 2: Reopen the database on the same data directory
    const opened = await tryOpen(dataDir)
    expect(opened.success).toBe(true)
    db = opened.db

    // Step 3: Verify general integrity (tables, indexes, basic queries)
    const integrity = await verifyIntegrity(db)
    expect(integrity.intact).toBe(true)

    // Step 4: Verify the baseline data is intact
    const baseline = await db.query(
      `SELECT * FROM items WHERE name LIKE 'baseline-%' ORDER BY id`,
    )

    // All 10 baseline rows should exist (none deleted by the rolled-back txn)
    expect(baseline.rows.length).toBe(10)

    // Values should be the original values (not modified by the rolled-back UPDATE)
    for (const row of baseline.rows) {
      const idx = parseInt(row.name.replace('baseline-', ''), 10)
      expect(row.value).toBe(idx * 10)
    }

    // Step 5: Verify the transaction's inserted rows are NOT present
    const txnRows = await db.query(
      `SELECT count(*)::int AS cnt FROM items WHERE name LIKE 'txn-%'`,
    )
    expect(txnRows.rows[0].cnt).toBe(0)

    // Total row count should be exactly the 10 baseline rows
    const total = await db.query(
      `SELECT count(*)::int AS cnt FROM items`,
    )
    expect(total.rows[0].cnt).toBe(10)
  })
})
