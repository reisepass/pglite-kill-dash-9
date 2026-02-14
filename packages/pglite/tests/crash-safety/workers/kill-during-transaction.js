/**
 * Worker: Kill during an explicit transaction (BEGIN...not yet COMMIT)
 *
 * 1. Creates a table and inserts baseline rows (committed)
 * 2. Starts a BEGIN block
 * 3. Inside the transaction: INSERTs, UPDATEs, DELETEs
 * 4. Sends 'in-transaction' so the parent can SIGKILL us before COMMIT
 */
import { PGlite } from '../../../dist/index.js'

const dataDir = process.env.PGLITE_DATA_DIR

async function run() {
  const db = new PGlite(dataDir)
  await db.waitReady

  // -- Phase 1: committed baseline data --
  await db.query(`
    CREATE TABLE IF NOT EXISTS items (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      value INT NOT NULL
    )
  `)

  // Insert 10 baseline rows (these are committed and should survive the crash)
  for (let i = 1; i <= 10; i++) {
    await db.query(`INSERT INTO items (name, value) VALUES ($1, $2)`, [
      `baseline-${i}`,
      i * 10,
    ])
  }

  // -- Phase 2: begin a transaction that will never commit --
  await db.query('BEGIN')

  // INSERT many new rows inside the transaction
  for (let i = 11; i <= 30; i++) {
    await db.query(`INSERT INTO items (name, value) VALUES ($1, $2)`, [
      `txn-row-${i}`,
      i * 100,
    ])
  }

  // UPDATE existing baseline rows inside the transaction
  await db.query(`UPDATE items SET value = value + 9999 WHERE name LIKE 'baseline-%'`)

  // DELETE some baseline rows inside the transaction
  await db.query(`DELETE FROM items WHERE name IN ('baseline-1', 'baseline-2')`)

  // Signal that we are in the middle of the transaction
  process.send('in-transaction')

  // Keep doing work so the process stays alive for the kill
  for (let i = 31; i <= 10000; i++) {
    await db.query(`INSERT INTO items (name, value) VALUES ($1, $2)`, [
      `txn-filler-${i}`,
      i,
    ])
  }

  // We should never reach here — parent will SIGKILL us
  await db.query('COMMIT')
  await db.close()
}

run().catch((err) => {
  console.error('Worker error:', err)
  process.exit(1)
})
