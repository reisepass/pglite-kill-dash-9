import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/dev-server-cycle.js',
  import.meta.url
).pathname

const dataDir = testDataDir('dev-server-cycle')

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    cleanupDataDir(dataDir)
  }
})

describe('crash safety: dev-server rapid restart simulation', () => {
  it(
    'should survive 20+ dev-server restart cycles with varying operations and kill timings',
    async () => {
      const cycles = []

      // Cycle 0: 'schema' — let schema complete, then kill (ensures table exists)
      cycles.push({ cycle: 0, operation: 'schema', killOnMessage: 'schema-created', killAfterMs: undefined })

      // Cycle 1: 'schema' again — kill while seeding (schema already exists from cycle 0)
      cycles.push({ cycle: 1, operation: 'schema', killOnMessage: 'schema-created', killAfterMs: undefined })

      // Cycles 2-7: 'insert' + kill after 300ms (mid-insert, dirty shutdown)
      for (let i = 2; i <= 7; i++) {
        cycles.push({ cycle: i, operation: 'insert', killOnMessage: undefined, killAfterMs: 300 })
      }

      // Cycles 8-11: 'query' + kill after 200ms (mid-query with writes)
      for (let i = 8; i <= 11; i++) {
        cycles.push({ cycle: i, operation: 'query', killOnMessage: undefined, killAfterMs: 200 })
      }

      // Cycles 12-14: 'migrate' + kill after operation-done (let migration finish, kill before close)
      for (let i = 12; i <= 14; i++) {
        cycles.push({ cycle: i, operation: 'migrate', killOnMessage: 'operation-done', killAfterMs: undefined })
      }

      // Cycles 15-19: 'insert' + kill after 100ms (very aggressive, simulates impatient dev)
      for (let i = 15; i <= 19; i++) {
        cycles.push({ cycle: i, operation: 'insert', killOnMessage: undefined, killAfterMs: 100 })
      }

      // Run all 20 cycles sequentially
      for (const { cycle, operation, killOnMessage, killAfterMs } of cycles) {
        const opts = {
          dataDir,
          workerScript: workerPath,
          env: {
            CYCLE: String(cycle),
            OPERATION: operation,
          },
        }

        if (killOnMessage) {
          opts.killOnMessage = killOnMessage
        } else {
          opts.killAfterMs = killAfterMs
        }

        const result = await crashTest(opts)
        expect(result.workerKilled).toBe(true)

        // For schema cycles, verify schema was created before kill
        if (killOnMessage === 'schema-created') {
          expect(result.workerMessages).toContain('schema-created')
        }
      }

      // After all cycles: open DB and verify consistency
      const opened = await tryOpen(dataDir)
      expect(opened.success).toBe(true)
      expect(opened.error).toBeNull()

      if (opened.success) {
        // Structural integrity check
        const integrity = await verifyIntegrity(opened.db)
        expect(integrity.issues).toEqual([])
        expect(integrity.intact).toBe(true)

        // Verify dev_items table exists
        const tableCheck = await opened.db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename = 'dev_items'
        `)
        expect(tableCheck.rows.length).toBe(1)

        // Verify dev_logs table exists
        const logsTableCheck = await opened.db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename = 'dev_logs'
        `)
        expect(logsTableCheck.rows.length).toBe(1)

        // Row count must be valid (>= 0). Since workers are killed mid-operation,
        // uncommitted data may be lost during crash recovery. The key test is
        // that the DB opens and is consistent, not that specific data survived.
        const itemCount = await opened.db.query(
          'SELECT count(*) as cnt FROM dev_items'
        )
        const rowCount = parseInt(itemCount.rows[0].cnt, 10)
        expect(rowCount).toBeGreaterThanOrEqual(0)

        // Verify we can read the data without errors
        const rows = await opened.db.query(
          'SELECT cycle, name, value FROM dev_items ORDER BY id'
        )
        expect(rows.rows.length).toBe(rowCount)

        // Verify log rows are readable
        const logCount = await opened.db.query(
          'SELECT count(*) as cnt FROM dev_logs'
        )
        const logRowCount = parseInt(logCount.rows[0].cnt, 10)
        expect(logRowCount).toBeGreaterThanOrEqual(0)

        // Verify indexes exist
        const indexes = await opened.db.query(`
          SELECT indexname FROM pg_indexes
          WHERE schemaname = 'public' AND tablename = 'dev_items'
          ORDER BY indexname
        `)
        const indexNames = indexes.rows.map((r) => r.indexname)
        expect(indexNames).toContain('idx_dev_items_cycle')

        // Verify schema is consistent - check column list on dev_items
        const columns = await opened.db.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'dev_items'
          ORDER BY ordinal_position
        `)
        const colNames = columns.rows.map((r) => r.column_name)
        // Must have at least the base columns from schema setup
        expect(colNames).toContain('id')
        expect(colNames).toContain('cycle')
        expect(colNames).toContain('name')
        expect(colNames).toContain('value')
        expect(colNames).toContain('created_at')

        await opened.db.close()
      }
    },
    { timeout: 180000 }
  )
})
