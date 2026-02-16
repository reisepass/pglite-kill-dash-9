import * as fs from 'fs'
import * as path from 'path'
import { EmscriptenBuiltinFilesystem, PGDATA } from './base.js'
import type { PostgresMod } from '../postgresMod.js'
import { PGlite } from '../pglite.js'

// TODO: Add locking for browser backends (OPFS-AHP via Web Locks API,
// IdbFS via Web Locks API). The Web Locks API (navigator.locks.request())
// provides exclusive locks that are automatically released when the
// tab/worker dies, making it ideal for browser-based PGlite instances.

export class NodeFS extends EmscriptenBuiltinFilesystem {
  protected rootDir: string
  #lockFd: number | null = null

  constructor(dataDir: string) {
    super(dataDir)
    this.rootDir = path.resolve(dataDir)
    if (!fs.existsSync(path.join(this.rootDir))) {
      fs.mkdirSync(this.rootDir)
    }
  }

  async init(pg: PGlite, opts: Partial<PostgresMod>) {
    this.pg = pg

    // Acquire exclusive lock before mounting the filesystem
    this.#acquireLock()

    // Detect and recover from partially-initialized data directories.
    // If initdb was interrupted by kill -9, the data directory may contain
    // partial files that PostgreSQL cannot recover from. We detect this by
    // checking for PG_VERSION (one of the last files initdb creates).
    // If the directory has files but no PG_VERSION, move to a backup
    // and let initdb start fresh.
    this.#cleanPartialInit()

    const options: Partial<PostgresMod> = {
      ...opts,
      preRun: [
        ...(opts.preRun || []),
        (mod: any) => {
          const nodefs = mod.FS.filesystems.NODEFS
          mod.FS.mkdir(PGDATA)
          mod.FS.mount(nodefs, { root: this.rootDir }, PGDATA)
        },
      ],
    }
    return { emscriptenOpts: options }
  }

  /**
   * Acquire an exclusive lock for this data directory.
   * The lock file is placed as a sibling to the data directory
   * (e.g., /path/to/mydb.lock for /path/to/mydb) to avoid
   * interfering with PostgreSQL's own data directory contents.
   */
  #acquireLock() {
    const lockPath = this.rootDir + '.lock'

    if (fs.existsSync(lockPath)) {
      try {
        const content = fs.readFileSync(lockPath, 'utf-8').trim()
        const lines = content.split('\n')
        const pid = parseInt(lines[0], 10)

        if (pid && !isNaN(pid) && this.#isProcessAlive(pid)) {
          throw new Error(
            `Data directory "${this.rootDir}" is locked by another PGlite instance ` +
              `(PID ${pid}). Close the other instance first, or delete ` +
              `"${lockPath}" if the process is no longer running.`,
          )
        }
        // Stale lock from a dead process — safe to take over
      } catch (e) {
        // Re-throw lock errors, ignore parse errors (corrupt lock file = stale)
        if (e instanceof Error && e.message.includes('is locked by')) {
          throw e
        }
      }
    }

    // Write our PID to the lock file and keep the fd open
    this.#lockFd = fs.openSync(lockPath, 'w')
    fs.writeSync(this.#lockFd, `${process.pid}\n${Date.now()}\n`)
  }

  #releaseLock() {
    if (this.#lockFd !== null) {
      try {
        fs.closeSync(this.#lockFd)
      } catch {
        // Ignore errors on close
      }
      this.#lockFd = null

      const lockPath = this.rootDir + '.lock'
      try {
        fs.unlinkSync(lockPath)
      } catch {
        // Ignore errors on unlink (dir may already be cleaned up)
      }
    }
  }

  #cleanPartialInit() {
    // Detect partially-initialized data directories from interrupted initdb.
    //
    // PostgreSQL's initdb creates files in stages:
    //   1. PG_VERSION + directory structure + postgresql.conf (~100ms)
    //   2. global/pg_control during bootstrap (~150ms)
    //   3. template1 database in base/1/ (ongoing during bootstrap)
    //   4. template0 database in base/4/ (copy of template1)
    //   5. postgres database in base/5/
    //
    // If killed during any stage, the database may have PG_VERSION and even
    // pg_control but be missing system views (pg_tables etc.) or entire
    // databases. A fully initialized PGlite always has 3+ databases in base/.
    // We detect partial init by checking for fewer than 3 base/ subdirectories.
    try {
      const entries = fs.readdirSync(this.rootDir)
      if (entries.length === 0) return

      const pgVersionPath = path.join(this.rootDir, 'PG_VERSION')
      if (!fs.existsSync(pgVersionPath)) {
        // Very early interruption — no PG_VERSION means initdb barely started
        this.#moveDataDirToBackup()
        return
      }

      // PG_VERSION exists — check if initdb completed by counting databases.
      // A complete initdb creates template1 (base/1), template0 (base/4),
      // and the user database (base/5). Fewer than 3 means partial init.
      const basePath = path.join(this.rootDir, 'base')
      if (fs.existsSync(basePath)) {
        const databases = fs.readdirSync(basePath)
        if (databases.length < 3) {
          this.#moveDataDirToBackup()
          return
        }
      } else {
        // base/ doesn't exist at all — very incomplete
        this.#moveDataDirToBackup()
      }
    } catch {
      // If we can't read the directory, let PostgreSQL handle the error
    }
  }

  #moveDataDirToBackup() {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const backupPath = `${this.rootDir}.corrupt-${timestamp}`
    fs.renameSync(this.rootDir, backupPath)
    fs.mkdirSync(this.rootDir)
    console.warn(
      `PGlite: Detected partially-initialized data directory. ` +
        `Moved to "${backupPath}" for recovery. A fresh database will be created.`,
    )
  }

  #isProcessAlive(pid: number): boolean {
    try {
      process.kill(pid, 0) // signal 0 = check if process exists
      return true
    } catch {
      return false // ESRCH = process doesn't exist
    }
  }

  async closeFs(): Promise<void> {
    this.#releaseLock()
    this.pg!.Module.FS.quit()
  }
}
