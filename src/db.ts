import { readdirSync, readFileSync } from "node:fs";
import pg from "pg";

pg.types.setTypeParser(pg.types.builtins.DATE, (value) => value);
pg.types.setTypeParser(pg.types.builtins.INT8, Number);

export function createPool(connectionString: string) {
  return new pg.Pool({ connectionString, max: 5 });
}

export async function migrate(pool: pg.Pool, dir = new URL("../migrations/", import.meta.url)) {
  const client = await pool.connect();
  try {
    await client.query("begin");
    await client.query("select pg_advisory_xact_lock(4242)");
    await client.query("create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())");
    const done = new Set((await client.query("select name from schema_migrations")).rows.map((r) => r.name));
    for (const name of readdirSync(dir).filter((f) => f.endsWith(".sql")).sort()) {
      if (done.has(name)) continue;
      await client.query(readFileSync(new URL(name, dir), "utf8"));
      await client.query("insert into schema_migrations (name) values ($1)", [name]);
    }
    await client.query("commit");
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}
