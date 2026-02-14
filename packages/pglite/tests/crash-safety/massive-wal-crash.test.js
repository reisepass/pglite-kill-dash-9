import { describe, it, expect, afterAll } from 'vitest'
import { statSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('massive-wal')

/**
 * Recursively compute the total size of a directory in bytes.
 */
function dirSizeBytes(dir) {
  let total = 0
  try {
    const entries = readdirSync(dir, { withFileTypes: true })
    for (const entry of entries) {
      const fullPath = join(dir, entry.name)
      if (entry.isDirectory()) {
        total += dirSizeBytes(fullPath)
      } else if (entry.isFile()) {
        total += statSync(fullPath).size
      }
    }
  } catch {
    // directory may not exist yet
  }
  return total
}

describe('Crash safety: massive WAL then kill (Issue #819)', () => {
  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it(
    'should recover after SIGKILL with a large dirty WAL',
    async () => {
      // Run the worker and kill it after the first UPDATE completes.
      // At that point we have 200 rows inserted + 200 rows updated, no
      // checkpoint, so the WAL should be large and dirty.
      const result = await crashTest({
        dataDir,
        workerScript: new URL(
          './workers/massive-wal-crash.js',
          import.meta.url
        ).pathname,
        killOnMessage: 'update-1-done',
      })

      expect(result.workerKilled).toBe(true)
      expect(result.workerMessages).toContain('insert-1-done')
      expect(result.workerMessages).toContain('update-1-done')

      // Measure datadir size before recovery
      const sizeBefore = dirSizeBytes(dataDir)

      // Try to reopen — with a large dirty WAL, recovery is harder and
      // slower, which is why we give a generous timeout
      const { success, db, error } = await tryOpen(dataDir, 60000)
      expect(success).toBe(true)
      expect(error).toBeNull()

      try {
        // Verify general integrity (tables scannable, indexes intact)
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        // The docs table should exist
        const tables = await db.query(
          `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'docs'`
        )
        expect(tables.rows.length).toBe(1)

        // Count rows: we killed after update-1-done, which means the first
        // INSERT (200 rows) and first UPDATE were committed. The second
        // INSERT and second UPDATE may or may not have happened.
        const total = await db.query(
          `SELECT count(*)::int AS cnt FROM docs`
        )
        const count = total.rows[0].cnt
        // We expect at least the first 200 rows to be present
        expect(count).toBeGreaterThanOrEqual(200)
        // And at most 400 (if second insert also made it before kill)
        expect(count).toBeLessThanOrEqual(400)

        // Verify the GIN index is usable by running a text search query
        const searchResult = await db.query(
          `SELECT count(*)::int AS cnt FROM docs WHERE body_tsv @@ to_tsquery('simple', 'title')`
        )
        expect(searchResult.rows[0].cnt).toBeGreaterThanOrEqual(1)

        // Verify data content — first batch rows should have their title
        const firstRow = await db.query(
          `SELECT title, length(body) AS bodylen FROM docs WHERE id = 1`
        )
        expect(firstRow.rows.length).toBe(1)
        expect(firstRow.rows[0].title).toBe('title-0')
        // Body should be at least 10KB (original size)
        expect(firstRow.rows[0].bodylen).toBeGreaterThanOrEqual(10000)

        // Measure datadir size after recovery
        const sizeAfter = dirSizeBytes(dataDir)

        // Log sizes for debugging — large WAL is the point of this test
        console.log(
          `Datadir size before recovery: ${(sizeBefore / 1024 / 1024).toFixed(2)} MB`
        )
        console.log(
          `Datadir size after recovery: ${(sizeAfter / 1024 / 1024).toFixed(2)} MB`
        )
        console.log(`Row count after recovery: ${count}`)
      } finally {
        await db.close()
      }
    },
    { timeout: 120000 }
  )
})
