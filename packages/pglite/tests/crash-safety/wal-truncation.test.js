import { describe, it, expect, afterAll, beforeAll } from 'vitest'
import {
  readFileSync,
  writeFileSync,
  readdirSync,
  statSync,
  unlinkSync,
  existsSync,
  cpSync,
} from 'node:fs'
import { join } from 'node:path'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'

const workerPath = new URL(
  './workers/wal-truncation.js',
  import.meta.url
).pathname

// We use a single "golden" crashed datadir, then copy it for each corruption test
const goldenDir = testDataDir('wal-trunc-golden')
const testDirs = []

function registerDir(name) {
  const dir = testDataDir(`wal-trunc-${name}`)
  testDirs.push(dir)
  return dir
}

/**
 * List WAL segment files in pg_wal/ directory.
 */
function getWalFiles(dataDir) {
  const walDir = join(dataDir, 'pg_wal')
  if (!existsSync(walDir)) return []
  return readdirSync(walDir)
    .filter((f) => /^[0-9A-F]{24}$/.test(f))
    .map((f) => ({
      name: f,
      path: join(walDir, f),
      size: statSync(join(walDir, f)).size,
    }))
    .sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * Copy a data directory to a new location.
 */
function copyDataDir(src, dst) {
  if (existsSync(dst)) {
    cleanupDataDir(dst)
  }
  cpSync(src, dst, { recursive: true })
}

/**
 * Find heap files in the base/ directory (the actual table data files).
 * Returns the largest files first (most likely to be user tables).
 */
function getHeapFiles(dataDir) {
  const baseDir = join(dataDir, 'base')
  if (!existsSync(baseDir)) return []
  const files = []
  for (const dbOid of readdirSync(baseDir)) {
    const dbDir = join(baseDir, dbOid)
    if (!statSync(dbDir).isDirectory()) continue
    for (const fname of readdirSync(dbDir)) {
      const fpath = join(dbDir, fname)
      // Heap files are numeric filenames (relfilenode)
      if (/^\d+$/.test(fname)) {
        const st = statSync(fpath)
        if (st.isFile() && st.size > 0) {
          files.push({ name: fname, path: fpath, size: st.size, dbOid })
        }
      }
    }
  }
  return files.sort((a, b) => b.size - a.size)
}

afterAll(async () => {
  if (!process.env.RETAIN_DATA) {
    cleanupDataDir(goldenDir)
    for (const dir of testDirs) {
      cleanupDataDir(dir)
    }
  }
})

describe('WAL truncation corruption attacks', () => {
  // Phase 1: Create the golden crashed datadir
  beforeAll(async () => {
    const result = await crashTest({
      dataDir: goldenDir,
      workerScript: workerPath,
      killOnMessage: 'main-updated',
    })

    expect(result.workerKilled).toBe(true)
    expect(result.workerMessages).toContain('ready')
    expect(result.workerMessages).toContain('main-updated')

    const walFiles = getWalFiles(goldenDir)
    console.log(
      `Golden dir WAL files: ${walFiles.map((f) => `${f.name} (${f.size}B)`).join(', ')}`
    )
    expect(walFiles.length).toBeGreaterThan(0)

    const heapFiles = getHeapFiles(goldenDir)
    console.log(
      `Golden dir largest heap files: ${heapFiles.slice(0, 5).map((f) => `${f.dbOid}/${f.name} (${f.size}B)`).join(', ')}`
    )
  }, 60000)

  // Sanity check: unmodified crashed dir recovers
  it(
    'baseline: unmodified crashed dir recovers successfully',
    async () => {
      const dataDir = registerDir('baseline')
      copyDataDir(goldenDir, dataDir)

      const { success, db, error } = await tryOpen(dataDir, 30000)
      expect(success).toBe(true)

      if (db) {
        const integrity = await verifyIntegrity(db)
        expect(integrity.intact).toBe(true)

        const count = await db.query(
          'SELECT count(*)::int AS cnt FROM wal_test_main'
        )
        console.log(`Baseline row count: ${count.rows[0].cnt}`)
        expect(count.rows[0].cnt).toBeGreaterThanOrEqual(100)
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 1: Truncate the FIRST WAL segment (contains critical early records)
  it(
    'truncate first WAL segment to 50% — corrupt early replay records',
    async () => {
      const dataDir = registerDir('trunc-first')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      // Target the FIRST WAL segment, not the last
      const target = walFiles[0]
      const original = readFileSync(target.path)
      const truncatedSize = Math.floor(original.length / 2)
      writeFileSync(target.path, original.slice(0, truncatedSize))
      console.log(`Truncated FIRST WAL ${target.name} from ${original.length} to ${truncatedSize}`)

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`First-segment truncation: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}, issues=${JSON.stringify(integrity.issues)}`)
          const count = await db.query('SELECT count(*)::int AS cnt FROM wal_test_main')
          console.log(`Row count: ${count.rows[0].cnt}`)
        } catch (e) {
          console.log(`Query error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 2: Corrupt ALL WAL segments with bit-flips at the start
  it(
    'corrupt header of every WAL segment — clobber WAL magic numbers',
    async () => {
      const dataDir = registerDir('corrupt-all-headers')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      for (const f of walFiles) {
        const buf = readFileSync(f.path)
        // Corrupt the first 512 bytes of each WAL file (header area)
        for (let i = 0; i < Math.min(512, buf.length); i++) {
          buf[i] = buf[i] ^ 0xFF
        }
        writeFileSync(f.path, buf)
      }
      console.log(`Corrupted headers of all ${walFiles.length} WAL segments`)

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`All-headers-corrupt: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 3: Delete ALL WAL segments
  it(
    'delete all WAL segments — total WAL loss',
    async () => {
      const dataDir = registerDir('delete-all')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      for (const f of walFiles) {
        unlinkSync(f.path)
      }
      console.log(`Deleted all ${walFiles.length} WAL segments`)

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Delete-all: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      } else {
        expect(success).toBe(false)
      }
    },
    { timeout: 60000 }
  )

  // Attack 4: Corrupt heap files + truncate WAL (combo attack)
  it(
    'corrupt largest heap files + truncate WAL — database pages + WAL both damaged',
    async () => {
      const dataDir = registerDir('heap-plus-wal')
      copyDataDir(goldenDir, dataDir)

      // Corrupt the 3 largest heap files
      const heapFiles = getHeapFiles(dataDir)
      for (const f of heapFiles.slice(0, 3)) {
        const buf = readFileSync(f.path)
        // Write garbage to the first 8KB of each heap file (page header)
        for (let i = 0; i < Math.min(8192, buf.length); i++) {
          buf[i] = Math.floor(Math.random() * 256)
        }
        writeFileSync(f.path, buf)
        console.log(`Corrupted heap file ${f.dbOid}/${f.name} (${f.size}B)`)
      }

      // Also truncate the last WAL segment
      const walFiles = getWalFiles(dataDir)
      if (walFiles.length > 0) {
        const target = walFiles[walFiles.length - 1]
        const original = readFileSync(target.path)
        writeFileSync(target.path, original.slice(0, 8192))
        console.log(`Truncated ${target.name} to 8KB`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Heap+WAL: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}, issues=${JSON.stringify(integrity.issues)}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 5: Swap two WAL segments (out of order replay)
  it(
    'swap two WAL segments — force out-of-order replay',
    async () => {
      const dataDir = registerDir('swap-wal')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      if (walFiles.length >= 2) {
        const first = walFiles[0]
        const second = walFiles[1]
        const firstData = readFileSync(first.path)
        const secondData = readFileSync(second.path)
        writeFileSync(first.path, secondData)
        writeFileSync(second.path, firstData)
        console.log(`Swapped ${first.name} <-> ${second.name}`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Swap-WAL: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 6: Zero out the ENTIRE middle WAL segment
  it(
    'zero entire middle WAL segment — gap in replay chain',
    async () => {
      const dataDir = registerDir('zero-middle')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      if (walFiles.length >= 3) {
        const midIdx = Math.floor(walFiles.length / 2)
        const target = walFiles[midIdx]
        writeFileSync(target.path, Buffer.alloc(target.size, 0))
        console.log(`Zeroed entire middle WAL ${target.name} (${target.size}B)`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Zero-middle: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 7: Corrupt pg_control to point to wrong WAL position
  it(
    'corrupt pg_control checkpoint LSN — mislead recovery start point',
    async () => {
      const dataDir = registerDir('corrupt-pgcontrol')
      copyDataDir(goldenDir, dataDir)

      const pgControlPath = join(dataDir, 'global', 'pg_control')
      if (existsSync(pgControlPath)) {
        const buf = readFileSync(pgControlPath)
        // pg_control stores checkpoint location near the start.
        // Corrupt bytes 8-24 which contain checkpoint/redo LSN pointers.
        for (let i = 8; i < Math.min(24, buf.length); i++) {
          buf[i] = 0xFF
        }
        writeFileSync(pgControlPath, buf)
        console.log(`Corrupted pg_control checkpoint LSN area`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Corrupt-pgcontrol: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 8: Truncate heap files (table data) while keeping WAL intact
  it(
    'truncate largest heap files to 0 — destroy table data, WAL intact',
    async () => {
      const dataDir = registerDir('trunc-heap')
      copyDataDir(goldenDir, dataDir)

      const heapFiles = getHeapFiles(dataDir)
      for (const f of heapFiles.slice(0, 5)) {
        writeFileSync(f.path, Buffer.alloc(0))
        console.log(`Truncated heap ${f.dbOid}/${f.name} to 0 (was ${f.size}B)`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Trunc-heap: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}, issues=${JSON.stringify(integrity.issues)}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 9: Delete middle WAL segment — gap in the chain
  it(
    'delete middle WAL segment — create gap in replay chain',
    async () => {
      const dataDir = registerDir('delete-middle')
      copyDataDir(goldenDir, dataDir)

      const walFiles = getWalFiles(dataDir)
      if (walFiles.length >= 3) {
        const midIdx = Math.floor(walFiles.length / 2)
        const target = walFiles[midIdx]
        unlinkSync(target.path)
        console.log(`Deleted middle WAL ${target.name} (${target.size}B)`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Delete-middle: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
          const count = await db.query('SELECT count(*)::int AS cnt FROM wal_test_main')
          console.log(`Row count: ${count.rows[0].cnt}`)
        } catch (e) {
          console.log(`Query error: ${e.message}`)
        }
        await db.close()
      }
    },
    { timeout: 60000 }
  )

  // Attack 10: Total destruction — corrupt all WAL + all large heap files + pg_control
  it(
    'total destruction — corrupt WAL + heap + pg_control simultaneously',
    async () => {
      const dataDir = registerDir('total-destruction')
      copyDataDir(goldenDir, dataDir)

      // Corrupt all WAL segments
      const walFiles = getWalFiles(dataDir)
      for (const f of walFiles) {
        const buf = Buffer.alloc(f.size)
        for (let i = 0; i < buf.length; i++) {
          buf[i] = Math.floor(Math.random() * 256)
        }
        writeFileSync(f.path, buf)
      }
      console.log(`Replaced all ${walFiles.length} WAL segments with random data`)

      // Corrupt heap files
      const heapFiles = getHeapFiles(dataDir)
      for (const f of heapFiles.slice(0, 5)) {
        const buf = readFileSync(f.path)
        for (let i = 0; i < buf.length; i++) {
          buf[i] = buf[i] ^ 0xFF
        }
        writeFileSync(f.path, buf)
      }
      console.log(`Bit-flipped ${Math.min(5, heapFiles.length)} largest heap files`)

      // Corrupt pg_control
      const pgControlPath = join(dataDir, 'global', 'pg_control')
      if (existsSync(pgControlPath)) {
        const buf = readFileSync(pgControlPath)
        for (let i = 0; i < buf.length; i++) {
          buf[i] = buf[i] ^ 0xAA
        }
        writeFileSync(pgControlPath, buf)
        console.log(`Corrupted pg_control entirely`)
      }

      const { success, db, error } = await tryOpen(dataDir, 30000)
      console.log(`Total-destruction: opened=${success}, error=${error?.message}`)
      if (success && db) {
        try {
          const integrity = await verifyIntegrity(db)
          console.log(`Integrity: intact=${integrity.intact}`)
        } catch (e) {
          console.log(`Integrity check error: ${e.message}`)
        }
        await db.close()
      } else {
        // This one should definitely fail
        expect(success).toBe(false)
      }
    },
    { timeout: 60000 }
  )
})
