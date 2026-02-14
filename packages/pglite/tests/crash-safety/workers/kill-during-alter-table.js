/**
 * Worker: Kill during ALTER TABLE
 *
 * Creates a table with data, then performs a series of ALTER TABLE operations
 * (ADD COLUMN, ADD COLUMN with DEFAULT, RENAME COLUMN, ADD CONSTRAINT).
 * The parent process will SIGKILL this process while ALTER TABLE is in progress.
 */

import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // Create the test table with some initial data
  await db.query(`
    CREATE TABLE IF NOT EXISTS alter_test (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      value INTEGER NOT NULL
    )
  `)

  // Insert some rows so the table has data that ALTER TABLE must handle
  for (let i = 0; i < 100; i++) {
    await db.query(
      `INSERT INTO alter_test (name, value) VALUES ($1, $2)`,
      [`item-${i}`, i * 10]
    )
  }

  // Signal that setup is done and we are about to start altering
  process.send('altering')

  // Perform several ALTER TABLE operations in sequence.
  // Each one touches the system catalog and may rewrite or update the table.

  // 1. Add a simple nullable column
  await db.query(`ALTER TABLE alter_test ADD COLUMN description TEXT`)

  // 2. Add a column with a DEFAULT value (causes a table rewrite in older PG,
  //    or a catalog-only change in newer PG, either way it's a schema change)
  await db.query(`ALTER TABLE alter_test ADD COLUMN score DOUBLE PRECISION DEFAULT 0.0`)

  // 3. Add another column with a non-null default
  await db.query(`ALTER TABLE alter_test ADD COLUMN status TEXT DEFAULT 'active' NOT NULL`)

  // 4. Rename a column
  await db.query(`ALTER TABLE alter_test RENAME COLUMN value TO amount`)

  // 5. Add a CHECK constraint
  await db.query(`ALTER TABLE alter_test ADD CONSTRAINT chk_amount CHECK (amount >= 0)`)

  // 6. Add a UNIQUE constraint (creates an index behind the scenes)
  await db.query(`ALTER TABLE alter_test ADD CONSTRAINT uq_name UNIQUE (name)`)

  // 7. Create a second table and add a foreign key
  await db.query(`
    CREATE TABLE IF NOT EXISTS alter_ref (
      id SERIAL PRIMARY KEY,
      alter_test_id INTEGER
    )
  `)
  await db.query(`
    ALTER TABLE alter_ref
      ADD CONSTRAINT fk_alter_test
      FOREIGN KEY (alter_test_id) REFERENCES alter_test(id)
  `)

  // 8. Drop a column
  await db.query(`ALTER TABLE alter_test DROP COLUMN description`)

  process.send('done')
  await db.close()
  process.exit(0)
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
