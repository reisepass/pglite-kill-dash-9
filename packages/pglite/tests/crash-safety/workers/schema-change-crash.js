/**
 * Worker: Schema change then crash (Issue #669)
 *
 * Accepts PHASE env var:
 *   - 'setup':   Create complex schema with multiple tables, indexes, and sample data
 *   - 'migrate': Open existing DB and perform schema migrations
 *   - 'verify':  Open DB and verify schema + data
 *
 * The parent process will SIGKILL this process at the appropriate moment.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR
const phase = process.env.PHASE || 'setup'

async function setup() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create users table
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL
    )
  `)

  // Create posts table with foreign key to users
  await db.query(`
    CREATE TABLE IF NOT EXISTS posts (
      id SERIAL PRIMARY KEY,
      user_id INTEGER REFERENCES users(id),
      title TEXT NOT NULL,
      body TEXT NOT NULL
    )
  `)

  // Create comments table with foreign key to posts
  await db.query(`
    CREATE TABLE IF NOT EXISTS comments (
      id SERIAL PRIMARY KEY,
      post_id INTEGER REFERENCES posts(id),
      body TEXT NOT NULL
    )
  `)

  // Create indexes
  await db.query(`CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)`)
  await db.query(`CREATE INDEX IF NOT EXISTS idx_posts_title ON posts(title)`)
  await db.query(`CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)`)

  // GIN index for full text search on posts.body
  await db.query(`
    CREATE INDEX IF NOT EXISTS idx_posts_body_fts
    ON posts USING gin(to_tsvector('english', body))
  `)

  // Insert 20 users
  for (let i = 0; i < 20; i++) {
    await db.query(
      `INSERT INTO users (name, email) VALUES ($1, $2)`,
      [`user-${i}`, `user-${i}@example.com`]
    )
  }

  // Insert 100 posts distributed across users
  for (let i = 0; i < 100; i++) {
    const userId = (i % 20) + 1
    await db.query(
      `INSERT INTO posts (user_id, title, body) VALUES ($1, $2, $3)`,
      [userId, `Post title ${i}`, `This is the body content for post number ${i} with some searchable text`]
    )
  }

  // Insert 500 comments distributed across posts
  for (let i = 0; i < 500; i++) {
    const postId = (i % 100) + 1
    await db.query(
      `INSERT INTO comments (post_id, body) VALUES ($1, $2)`,
      [postId, `Comment ${i} on the post with some discussion content`]
    )
  }

  process.send('ready')
  await db.close()
  process.exit(0)
}

async function migrate() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Signal that we are about to start migrations
  process.send('migrating')

  // Perform schema changes -- the process may be killed mid-way through
  await db.query(`ALTER TABLE users ADD COLUMN avatar_url TEXT`)
  await db.query(`ALTER TABLE posts ADD COLUMN published_at TIMESTAMP`)
  await db.query(`CREATE INDEX idx_posts_published_at ON posts(published_at)`)
  await db.query(`ALTER TABLE comments ADD COLUMN upvotes INTEGER DEFAULT 0`)
  await db.query(`DROP INDEX idx_posts_title`)
  await db.query(`UPDATE posts SET published_at = now()`)

  process.send('done')
  await db.close()
  process.exit(0)
}

async function verify() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Check tables exist
  const tables = await db.query(`
    SELECT tablename FROM pg_tables
    WHERE schemaname = 'public'
    ORDER BY tablename
  `)
  const tableNames = tables.rows.map((r) => r.tablename)
  process.send({ type: 'tables', data: tableNames })

  // Check columns on users
  const userCols = await db.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'users'
    ORDER BY ordinal_position
  `)
  process.send({ type: 'user_columns', data: userCols.rows.map((r) => r.column_name) })

  // Check row counts
  const userCount = await db.query(`SELECT count(*)::int as cnt FROM users`)
  const postCount = await db.query(`SELECT count(*)::int as cnt FROM posts`)
  const commentCount = await db.query(`SELECT count(*)::int as cnt FROM comments`)
  process.send({
    type: 'counts',
    data: {
      users: userCount.rows[0].cnt,
      posts: postCount.rows[0].cnt,
      comments: commentCount.rows[0].cnt,
    },
  })

  process.send('verify-done')
  await db.close()
  process.exit(0)
}

const phases = { setup, migrate, verify }
const fn = phases[phase]

if (!fn) {
  console.error(`Unknown phase: ${phase}`)
  process.exit(1)
}

fn().catch((err) => {
  console.error(`Worker error (phase=${phase}):`, err)
  process.exit(1)
})
