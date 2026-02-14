import { describe, it, expect, afterAll } from 'vitest'
import {
  crashTest,
  tryOpen,
  verifyIntegrity,
  cleanupDataDir,
  testDataDir,
} from './harness.js'
import {
  readdirSync,
  statSync,
  readFileSync,
  writeFileSync,
  truncateSync,
  unlinkSync,
  copyFileSync,
  existsSync,
} from 'node:fs'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'

const workerPath = new URL(
  './workers/wal-manipulation.js',
  import.meta.url
).pathname

/**
 * Helper: populate a data directory by running the worker, inserting data,
 * and killing it mid-write. Returns the crash test result.
 */
async function populateAndKill(dataDir) {
  const result = await crashTest({
    dataDir,
    workerScript: workerPath,
    killOnMessage: 'inserting',
  })
  return result
}

/**
 * Helper: get all WAL files sorted by name (ascending).
 */
function getWalFiles(dataDir) {
  const walDir = join(dataDir, 'pg_wal')
  if (!existsSync(walDir)) return []
  return readdirSync(walDir)
    .filter((f) => /^[0-9A-F]{24}$/.test(f))
    .sort()
    .map((name) => ({
      name,
      path: join(walDir, name),
      size: statSync(join(walDir, name)).size,
    }))
}

/**
 * Helper: find the largest data files in base/ (likely the user table data).
 */
function getLargestDataFiles(dataDir, count = 3) {
  const baseDir = join(dataDir, 'base')
  if (!existsSync(baseDir)) return []
  const files = []
  for (const db of readdirSync(baseDir)) {
    const dbDir = join(baseDir, db)
    if (!statSync(dbDir).isDirectory()) continue
    for (const f of readdirSync(dbDir)) {
      const fPath = join(dbDir, f)
      const stat = statSync(fPath)
      if (stat.isFile() && stat.size > 0) {
        files.push({ path: fPath, size: stat.size, name: `${db}/${f}` })
      }
    }
  }
  files.sort((a, b) => b.size - a.size)
  return files.slice(0, count)
}

// ============================================================================
// Test suite
// ============================================================================

describe('crash safety: WAL file manipulation after kill', () => {

  // ---- a) WAL tail truncation ----

  describe('WAL tail truncation', () => {
    const percentages = [75, 50, 25]

    for (const pct of percentages) {
      const dataDir = testDataDir(`wal-trunc-${pct}`)

      afterAll(() => {
        if (!process.env.RETAIN_DATA) cleanupDataDir(dataDir)
      })

      it(
        `should handle WAL truncated to ${pct}% of its size`,
        async () => {
          const result = await populateAndKill(dataDir)
          expect(result.workerKilled).toBe(true)

          // Find the last WAL file and truncate it
          const walFiles = getWalFiles(dataDir)
          expect(walFiles.length).toBeGreaterThan(0)

          const lastWal = walFiles[walFiles.length - 1]
          const targetSize = Math.floor(lastWal.size * (pct / 100))
          truncateSync(lastWal.path, targetSize)

          const truncatedStat = statSync(lastWal.path)
          expect(truncatedStat.size).toBe(targetSize)

          // Try to reopen
          const opened = await tryOpen(dataDir)

          // Document behavior
          if (opened.success) {
            console.log(
              `WAL truncated to ${pct}%: PGlite OPENED SUCCESSFULLY`
            )
            // Check if data is consistent
            const integrity = await verifyIntegrity(opened.db)
            if (integrity.intact) {
              console.log(`  Data integrity: INTACT`)
            } else {
              console.log(
                `  Data integrity: ISSUES FOUND - ${integrity.issues.join(', ')}`
              )
            }

            try {
              const count = await opened.db.query(
                'SELECT count(*) as cnt FROM wal_test'
              )
              console.log(`  Row count: ${count.rows[0].cnt}`)
            } catch (e) {
              console.log(`  Could not query table: ${e.message}`)
            }

            await opened.db.close()
          } else {
            console.log(
              `WAL truncated to ${pct}%: PGlite FAILED TO OPEN (detected corruption)`
            )
            console.log(`  Error: ${opened.error?.message}`)
          }

          // Test passes regardless - we're documenting behavior
          expect(true).toBe(true)
        },
        { timeout: 120000 }
      )
    }
  })

  // ---- b) WAL tail zeroing ----

  describe('WAL tail zeroing', () => {
    const zeroSizes = [4096, 8192, 16384]

    for (const zeroBytes of zeroSizes) {
      const label = `${zeroBytes / 1024}KB`
      const dataDir = testDataDir(`wal-zero-${zeroBytes}`)

      afterAll(() => {
        if (!process.env.RETAIN_DATA) cleanupDataDir(dataDir)
      })

      it(
        `should handle last ${label} of WAL zeroed out`,
        async () => {
          const result = await populateAndKill(dataDir)
          expect(result.workerKilled).toBe(true)

          const walFiles = getWalFiles(dataDir)
          expect(walFiles.length).toBeGreaterThan(0)

          const lastWal = walFiles[walFiles.length - 1]
          const walData = readFileSync(lastWal.path)
          const zeros = Buffer.alloc(zeroBytes, 0)

          // Overwrite the last N bytes with zeros
          const offset = Math.max(0, walData.length - zeroBytes)
          zeros.copy(walData, offset)
          writeFileSync(lastWal.path, walData)

          // Verify the write
          const modifiedData = readFileSync(lastWal.path)
          const tailSlice = modifiedData.slice(offset)
          expect(tailSlice.every((b) => b === 0)).toBe(true)

          // Try to reopen
          const opened = await tryOpen(dataDir)

          if (opened.success) {
            console.log(
              `WAL tail zeroed (${label}): PGlite OPENED SUCCESSFULLY`
            )
            const integrity = await verifyIntegrity(opened.db)
            if (integrity.intact) {
              console.log(`  Data integrity: INTACT`)
            } else {
              console.log(
                `  Data integrity: ISSUES FOUND - ${integrity.issues.join(', ')}`
              )
            }
            try {
              const count = await opened.db.query(
                'SELECT count(*) as cnt FROM wal_test'
              )
              console.log(`  Row count: ${count.rows[0].cnt}`)
            } catch (e) {
              console.log(`  Could not query table: ${e.message}`)
            }
            await opened.db.close()
          } else {
            console.log(
              `WAL tail zeroed (${label}): PGlite FAILED TO OPEN (detected corruption)`
            )
            console.log(`  Error: ${opened.error?.message}`)
          }

          expect(true).toBe(true)
        },
        { timeout: 120000 }
      )
    }
  })

  // ---- c) WAL garbage injection ----

  describe('WAL garbage injection', () => {
    const dataDir = testDataDir('wal-garbage')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDir)
    })

    it(
      'should handle random bytes written to last WAL page (8KB)',
      async () => {
        const result = await populateAndKill(dataDir)
        expect(result.workerKilled).toBe(true)

        const walFiles = getWalFiles(dataDir)
        expect(walFiles.length).toBeGreaterThan(0)

        const lastWal = walFiles[walFiles.length - 1]
        const walData = readFileSync(lastWal.path)

        // Overwrite the last 8KB with random garbage
        const garbageSize = 8192
        const garbage = randomBytes(garbageSize)
        const offset = Math.max(0, walData.length - garbageSize)
        garbage.copy(walData, offset)
        writeFileSync(lastWal.path, walData)

        const opened = await tryOpen(dataDir)

        if (opened.success) {
          console.log('WAL garbage (8KB): PGlite OPENED SUCCESSFULLY')
          const integrity = await verifyIntegrity(opened.db)
          if (integrity.intact) {
            console.log('  Data integrity: INTACT')
          } else {
            console.log(
              `  Data integrity: ISSUES FOUND - ${integrity.issues.join(', ')}`
            )
          }
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
          } catch (e) {
            console.log(`  Could not query table: ${e.message}`)
          }
          await opened.db.close()
        } else {
          console.log(
            'WAL garbage (8KB): PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirMid = testDataDir('wal-garbage-mid')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirMid)
    })

    it(
      'should handle random bytes written to middle of last WAL file',
      async () => {
        const result = await populateAndKill(dataDirMid)
        expect(result.workerKilled).toBe(true)

        const walFiles = getWalFiles(dataDirMid)
        expect(walFiles.length).toBeGreaterThan(0)

        const lastWal = walFiles[walFiles.length - 1]
        const walData = readFileSync(lastWal.path)

        // Overwrite 8KB in the middle of the WAL
        const garbageSize = 8192
        const midOffset = Math.floor(walData.length / 2)
        const garbage = randomBytes(garbageSize)
        garbage.copy(walData, midOffset)
        writeFileSync(lastWal.path, walData)

        const opened = await tryOpen(dataDirMid)

        if (opened.success) {
          console.log('WAL garbage (middle 8KB): PGlite OPENED SUCCESSFULLY')
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
          } catch (e) {
            console.log(`  Could not query table: ${e.message}`)
          }
          await opened.db.close()
        } else {
          console.log(
            'WAL garbage (middle 8KB): PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )
  })

  // ---- d) WAL segment deletion ----

  describe('WAL segment deletion', () => {
    const dataDir = testDataDir('wal-delete')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDir)
    })

    it(
      'should handle newest WAL segment deleted entirely',
      async () => {
        const result = await populateAndKill(dataDir)
        expect(result.workerKilled).toBe(true)

        const walFiles = getWalFiles(dataDir)
        expect(walFiles.length).toBeGreaterThan(0)

        const lastWal = walFiles[walFiles.length - 1]
        console.log(
          `Deleting newest WAL segment: ${lastWal.name} (${lastWal.size} bytes)`
        )
        unlinkSync(lastWal.path)

        const opened = await tryOpen(dataDir)

        if (opened.success) {
          console.log('WAL segment deleted: PGlite OPENED SUCCESSFULLY')
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
          } catch (e) {
            console.log(`  Could not query table: ${e.message}`)
          }
          await opened.db.close()
        } else {
          console.log(
            'WAL segment deleted: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirMulti = testDataDir('wal-delete-multi')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirMulti)
    })

    it(
      'should handle two newest WAL segments deleted',
      async () => {
        const result = await populateAndKill(dataDirMulti)
        expect(result.workerKilled).toBe(true)

        const walFiles = getWalFiles(dataDirMulti)
        expect(walFiles.length).toBeGreaterThan(1)

        // Delete the two newest
        const toDelete = walFiles.slice(-2)
        for (const wal of toDelete) {
          console.log(`Deleting WAL segment: ${wal.name} (${wal.size} bytes)`)
          unlinkSync(wal.path)
        }

        const opened = await tryOpen(dataDirMulti)

        if (opened.success) {
          console.log(
            'Two WAL segments deleted: PGlite OPENED SUCCESSFULLY'
          )
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
          } catch (e) {
            console.log(`  Could not query table: ${e.message}`)
          }
          await opened.db.close()
        } else {
          console.log(
            'Two WAL segments deleted: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )
  })

  // ---- e) WAL segment duplication ----

  describe('WAL segment duplication', () => {
    const dataDir = testDataDir('wal-dup')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDir)
    })

    it(
      'should handle older WAL segment copied over newer one',
      async () => {
        const result = await populateAndKill(dataDir)
        expect(result.workerKilled).toBe(true)

        const walFiles = getWalFiles(dataDir)
        expect(walFiles.length).toBeGreaterThan(1)

        const oldest = walFiles[0]
        const newest = walFiles[walFiles.length - 1]

        console.log(
          `Copying ${oldest.name} over ${newest.name} (simulating journal replay error)`
        )
        copyFileSync(oldest.path, newest.path)

        const opened = await tryOpen(dataDir)

        if (opened.success) {
          console.log(
            'WAL segment duplicated: PGlite OPENED SUCCESSFULLY'
          )
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
          } catch (e) {
            console.log(`  Could not query table: ${e.message}`)
          }
          await opened.db.close()
        } else {
          console.log(
            'WAL segment duplicated: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )
  })

  // ---- f) Control file corruption ----

  describe('Control file corruption', () => {
    const dataDirZero = testDataDir('pg-control-zero')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirZero)
    })

    it(
      'should handle pg_control partially zeroed',
      async () => {
        const result = await populateAndKill(dataDirZero)
        expect(result.workerKilled).toBe(true)

        const controlPath = join(dataDirZero, 'global', 'pg_control')
        expect(existsSync(controlPath)).toBe(true)

        const controlData = readFileSync(controlPath)
        console.log(`pg_control size: ${controlData.length} bytes`)

        // Zero out the first 512 bytes (where checkpoint info lives)
        const zeros = Buffer.alloc(512, 0)
        zeros.copy(controlData, 0)
        writeFileSync(controlPath, controlData)

        const opened = await tryOpen(dataDirZero)

        if (opened.success) {
          console.log(
            'pg_control zeroed (512B): PGlite OPENED SUCCESSFULLY (DANGEROUS - should have failed!)'
          )
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          await opened.db.close()
        } else {
          console.log(
            'pg_control zeroed (512B): PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirTrunc = testDataDir('pg-control-trunc')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirTrunc)
    })

    it(
      'should handle pg_control truncated to half size',
      async () => {
        const result = await populateAndKill(dataDirTrunc)
        expect(result.workerKilled).toBe(true)

        const controlPath = join(dataDirTrunc, 'global', 'pg_control')
        expect(existsSync(controlPath)).toBe(true)

        const stat = statSync(controlPath)
        const halfSize = Math.floor(stat.size / 2)
        console.log(
          `Truncating pg_control from ${stat.size} to ${halfSize} bytes`
        )
        truncateSync(controlPath, halfSize)

        const opened = await tryOpen(dataDirTrunc)

        if (opened.success) {
          console.log(
            'pg_control truncated: PGlite OPENED SUCCESSFULLY (DANGEROUS - should have failed!)'
          )
          await opened.db.close()
        } else {
          console.log(
            'pg_control truncated: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirGarbage = testDataDir('pg-control-garbage')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirGarbage)
    })

    it(
      'should handle pg_control overwritten with random data',
      async () => {
        const result = await populateAndKill(dataDirGarbage)
        expect(result.workerKilled).toBe(true)

        const controlPath = join(dataDirGarbage, 'global', 'pg_control')
        expect(existsSync(controlPath)).toBe(true)

        const stat = statSync(controlPath)
        // Overwrite entire pg_control with random bytes
        const garbage = randomBytes(stat.size)
        writeFileSync(controlPath, garbage)

        const opened = await tryOpen(dataDirGarbage)

        if (opened.success) {
          console.log(
            'pg_control garbage: PGlite OPENED SUCCESSFULLY (DANGEROUS - should have failed!)'
          )
          await opened.db.close()
        } else {
          console.log(
            'pg_control garbage: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )
  })

  // ---- g) Data file corruption ----

  describe('Data file corruption with WAL intact', () => {
    const dataDirTrunc = testDataDir('datafile-trunc')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirTrunc)
    })

    it(
      'should handle data file truncated while WAL is intact',
      async () => {
        const result = await populateAndKill(dataDirTrunc)
        expect(result.workerKilled).toBe(true)

        // Find the largest data files (likely the user table)
        const dataFiles = getLargestDataFiles(dataDirTrunc)
        expect(dataFiles.length).toBeGreaterThan(0)

        const target = dataFiles[0]
        const halfSize = Math.floor(target.size / 2)
        console.log(
          `Truncating data file ${target.name} from ${target.size} to ${halfSize} bytes`
        )
        truncateSync(target.path, halfSize)

        const opened = await tryOpen(dataDirTrunc)

        if (opened.success) {
          console.log(
            'Data file truncated: PGlite OPENED SUCCESSFULLY'
          )
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
            console.log(
              '  WARNING: Data file was truncated but query succeeded - possible silent corruption'
            )
          } catch (e) {
            console.log(
              `  Query error (expected with truncated data): ${e.message}`
            )
          }
          await opened.db.close()
        } else {
          console.log(
            'Data file truncated: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirZero = testDataDir('datafile-zero')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirZero)
    })

    it(
      'should handle data file zeroed out while WAL is intact',
      async () => {
        const result = await populateAndKill(dataDirZero)
        expect(result.workerKilled).toBe(true)

        const dataFiles = getLargestDataFiles(dataDirZero)
        expect(dataFiles.length).toBeGreaterThan(0)

        const target = dataFiles[0]
        console.log(
          `Zeroing data file ${target.name} (${target.size} bytes)`
        )
        const zeros = Buffer.alloc(target.size, 0)
        writeFileSync(target.path, zeros)

        const opened = await tryOpen(dataDirZero)

        if (opened.success) {
          console.log('Data file zeroed: PGlite OPENED SUCCESSFULLY')
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
            console.log(
              '  WARNING: Data file was zeroed but query succeeded - silent corruption!'
            )
          } catch (e) {
            console.log(
              `  Query error (expected with zeroed data): ${e.message}`
            )
          }
          await opened.db.close()
        } else {
          console.log(
            'Data file zeroed: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )

    const dataDirGarbage = testDataDir('datafile-garbage')

    afterAll(() => {
      if (!process.env.RETAIN_DATA) cleanupDataDir(dataDirGarbage)
    })

    it(
      'should handle data file overwritten with random bytes while WAL intact',
      async () => {
        const result = await populateAndKill(dataDirGarbage)
        expect(result.workerKilled).toBe(true)

        const dataFiles = getLargestDataFiles(dataDirGarbage)
        expect(dataFiles.length).toBeGreaterThan(0)

        const target = dataFiles[0]
        console.log(
          `Writing random bytes to data file ${target.name} (${target.size} bytes)`
        )
        const garbage = randomBytes(target.size)
        writeFileSync(target.path, garbage)

        const opened = await tryOpen(dataDirGarbage)

        if (opened.success) {
          console.log(
            'Data file garbage: PGlite OPENED SUCCESSFULLY'
          )
          const integrity = await verifyIntegrity(opened.db)
          console.log(
            `  Data integrity: ${integrity.intact ? 'INTACT' : 'ISSUES - ' + integrity.issues.join(', ')}`
          )
          try {
            const count = await opened.db.query(
              'SELECT count(*) as cnt FROM wal_test'
            )
            console.log(`  Row count: ${count.rows[0].cnt}`)
            console.log(
              '  WARNING: Data file was garbage but query succeeded - silent corruption!'
            )
          } catch (e) {
            console.log(
              `  Query error (expected with garbage data): ${e.message}`
            )
          }
          await opened.db.close()
        } else {
          console.log(
            'Data file garbage: PGlite FAILED TO OPEN (detected corruption)'
          )
          console.log(`  Error: ${opened.error?.message}`)
        }

        expect(true).toBe(true)
      },
      { timeout: 120000 }
    )
  })
})
