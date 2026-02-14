import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('alter-table')

describe('crash during ALTER TABLE', () => {
  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  it(
    'should recover after kill during ALTER TABLE operations',
    async () => {
      // 1. Run the worker and kill it when it signals 'altering'
      const result = await crashTest({
        dataDir,
        workerScript: new URL(
          './workers/kill-during-alter-table.js',
          import.meta.url,
        ).pathname,
        killOnMessage: 'altering',
      })

      expect(result.workerKilled).toBe(true)

      // 2. Try to reopen the database after the crash
      const { success, db, error } = await tryOpen(dataDir)
      expect(success).toBe(true)
      expect(error).toBeNull()
      expect(db).not.toBeNull()

      try {
        // 3. Verify general database integrity
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        // 4. The original table should exist and be queryable
        const tables = await db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public'
          ORDER BY tablename
        `)
        const tableNames = tables.rows.map((r) => r.tablename)
        expect(tableNames).toContain('alter_test')

        // 5. Original data should be accessible -- the 100 rows from setup
        const count = await db.query('SELECT count(*)::int as cnt FROM alter_test')
        expect(count.rows[0].cnt).toBe(100)

        // 6. Verify the original columns are present (at minimum id, name, value/amount)
        const cols = await db.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'alter_test'
          ORDER BY ordinal_position
        `)
        const colNames = cols.rows.map((r) => r.column_name)
        expect(colNames).toContain('id')
        // 'name' should always be present
        expect(colNames).toContain('name')
        // Depending on how far the worker got, the column may still be
        // 'value' (rename not yet applied) or 'amount' (rename applied).
        const hasValueOrAmount =
          colNames.includes('value') || colNames.includes('amount')
        expect(hasValueOrAmount).toBe(true)

        // 7. Ensure we can still write to the table after recovery
        await db.query(
          `INSERT INTO alter_test (name, ${colNames.includes('amount') ? 'amount' : 'value'}) VALUES ($1, $2)`,
          ['recovery-test', 999],
        )
        const after = await db.query('SELECT count(*)::int as cnt FROM alter_test')
        expect(after.rows[0].cnt).toBe(101)

        // 8. Check that the schema is consistent by querying pg_class
        const pgClassCheck = await db.query(`
          SELECT relname FROM pg_class
          WHERE relname = 'alter_test' AND relkind = 'r'
        `)
        expect(pgClassCheck.rows.length).toBe(1)
      } finally {
        await db.close()
      }
    },
    { timeout: 60000 },
  )
})
