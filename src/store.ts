import { mkdirSync, chmodSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";
export class Store {
  readonly db: DatabaseSync;
  constructor(path: string) {
    if (path !== ":memory:")
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(path);
    if (path !== ":memory:") chmodSync(path, 0o600);
    this.db
      .exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS state (bucket TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(bucket,key));`);
  }
  get<T>(bucket: string, key: string): T | undefined {
    const row = this.db
      .prepare("SELECT value FROM state WHERE bucket=? AND key=?")
      .get(bucket, key);
    return row ? (JSON.parse(String(row.value)) as T) : undefined;
  }
  set(bucket: string, key: string, value: unknown): void {
    this.db
      .prepare(
        "INSERT INTO state VALUES(?,?,?) ON CONFLICT(bucket,key) DO UPDATE SET value=excluded.value",
      )
      .run(bucket, key, JSON.stringify(value));
  }
  delete(bucket: string, key: string): void {
    this.db
      .prepare("DELETE FROM state WHERE bucket=? AND key=?")
      .run(bucket, key);
  }
  all<T>(bucket: string): Array<[string, T]> {
    return this.db
      .prepare("SELECT key,value FROM state WHERE bucket=? ORDER BY key")
      .all(bucket)
      .map((r) => [String(r.key), JSON.parse(String(r.value)) as T]);
  }
  transaction<T>(fn: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = fn();
      this.db.exec("COMMIT");
      return result;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  close(): void {
    this.db.close();
  }
}
