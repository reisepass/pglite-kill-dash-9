import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const dataDir = testDataDir('schema-change')

describe('schema change then crash then reopen (issue #669)', () => {
  afterAll(async () => {
    if (!process.env.RETAIN_DATA) {
      cleanupDataDir(dataDir)
    }
  })

  const workerScript = new URL(
    './workers/schema-change-crash.js',
    import.meta.url,
  ).pathname

  it(
    'should recover after crash during schema migration',
    async () => {
      // Phase 1: Setup -- create complex schema and populate data, let it finish
      const setupResult = await crashTest({
        dataDir,
        workerScript,
        killOnMessage: 'ready',
        env: { PHASE: 'setup' },
      })

      // The worker should have been killed after sending 'ready'
      expect(setupResult.workerKilled).toBe(true)
      expect(setupResult.workerMessages).toContain('ready')

      // Verify the DB opens cleanly after setup phase kill
      {
        const { success, db, error } = await tryOpen(dataDir)
        expect(success).toBe(true)
        expect(error).toBeNull()

        try {
          const count = await db.query('SELECT count(*)::int as cnt FROM users')
          expect(count.rows[0].cnt).toBe(20)

          const postCount = await db.query('SELECT count(*)::int as cnt FROM posts')
          expect(postCount.rows[0].cnt).toBe(100)

          const commentCount = await db.query('SELECT count(*)::int as cnt FROM comments')
          expect(commentCount.rows[0].cnt).toBe(500)
        } finally {
          await db.close()
        }
      }

      // Phase 2: Migrate -- start schema changes and SIGKILL mid-migration
      const migrateResult = await crashTest({
        dataDir,
        workerScript,
        killOnMessage: 'migrating',
        signal: 'SIGKILL',
        env: { PHASE: 'migrate' },
      })

      expect(migrateResult.workerKilled).toBe(true)
      expect(migrateResult.workerMessages).toContain('migrating')

      // Phase 3: Reopen and verify the database is consistent
      const { success, db, error } = await tryOpen(dataDir)
      expect(success).toBe(true)
      expect(error).toBeNull()
      expect(db).not.toBeNull()

      try {
        // General integrity check
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        // All three tables must exist
        const tables = await db.query(`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public'
          ORDER BY tablename
        `)
        const tableNames = tables.rows.map((r) => r.tablename)
        expect(tableNames).toContain('users')
        expect(tableNames).toContain('posts')
        expect(tableNames).toContain('comments')

        // Original data must be intact
        const userCount = await db.query('SELECT count(*)::int as cnt FROM users')
        expect(userCount.rows[0].cnt).toBe(20)

        const postCount = await db.query('SELECT count(*)::int as cnt FROM posts')
        expect(postCount.rows[0].cnt).toBe(100)

        const commentCount = await db.query('SELECT count(*)::int as cnt FROM comments')
        expect(commentCount.rows[0].cnt).toBe(500)

        // Check users columns -- original columns must be present
        const userCols = await db.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'users'
          ORDER BY ordinal_position
        `)
        const userColNames = userCols.rows.map((r) => r.column_name)
        expect(userColNames).toContain('id')
        expect(userColNames).toContain('name')
        expect(userColNames).toContain('email')

        // avatar_url may or may not be present depending on how far migration got
        // -- we just note it, not assert it

        // Check posts columns
        const postCols = await db.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'posts'
          ORDER BY ordinal_position
        `)
        const postColNames = postCols.rows.map((r) => r.column_name)
        expect(postColNames).toContain('id')
        expect(postColNames).toContain('user_id')
        expect(postColNames).toContain('title')
        expect(postColNames).toContain('body')

        // Check comments columns
        const commentCols = await db.query(`
          SELECT column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = 'comments'
          ORDER BY ordinal_position
        `)
        const commentColNames = commentCols.rows.map((r) => r.column_name)
        expect(commentColNames).toContain('id')
        expect(commentColNames).toContain('post_id')
        expect(commentColNames).toContain('body')

        // The GIN index on posts.body should still work -- run a full text query
        const ftsResult = await db.query(`
          SELECT count(*)::int as cnt FROM posts
          WHERE to_tsvector('english', body) @@ to_tsquery('english', 'searchable')
        `)
        expect(ftsResult.rows[0].cnt).toBeGreaterThan(0)

        // Verify we can still write to all tables after recovery
        await db.query(
          `INSERT INTO users (name, email) VALUES ($1, $2)`,
          ['recovery-user', 'recovery@example.com'],
        )
        const afterUserCount = await db.query('SELECT count(*)::int as cnt FROM users')
        expect(afterUserCount.rows[0].cnt).toBe(21)

        await db.query(
          `INSERT INTO posts (user_id, title, body) VALUES ($1, $2, $3)`,
          [1, 'Recovery post', 'Testing write after crash recovery'],
        )
        const afterPostCount = await db.query('SELECT count(*)::int as cnt FROM posts')
        expect(afterPostCount.rows[0].cnt).toBe(101)

        // Verify pg_class is consistent
        const pgClassCheck = await db.query(`
          SELECT relname FROM pg_class
          WHERE relname IN ('users', 'posts', 'comments') AND relkind = 'r'
          ORDER BY relname
        `)
        expect(pgClassCheck.rows.length).toBe(3)
      } finally {
        await db.close()
      }
    },
    { timeout: 120000 },
  )
})
