import { randomUUID } from "node:crypto";
import type { CommandEnvelopeV1 } from "@famashines/the-vps-contracts";
import type pg from "pg";

export type Command<P> = CommandEnvelopeV1 & { payload: P };
export type Result<T> = { value: T; replayed: boolean };
export type Meta = { correlationId: string; causationId: string; issuedBy: string | null; occurredAt: Date };

export class Refused extends Error {
  readonly status: 404 | 409 | 422;
  readonly field?: string;
  constructor(status: 404 | 409 | 422, message: string, field?: string) {
    super(message);
    this.status = status;
    this.field = field;
  }
}

const messages: Record<string, [404 | 409, string]> = {
  fee_schedules_year_fk: [404, "The school year does not exist."],
};

export async function transaction<T>(pool: pg.Pool, work: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const value = await work(client);
    await client.query("commit");
    return value;
  } catch (error) {
    await client.query("rollback");
    const known = messages[(error as { constraint?: string }).constraint ?? ""];
    if (known) throw new Refused(known[0], known[1]);
    throw error;
  } finally {
    client.release();
  }
}

export async function append(client: pg.PoolClient, meta: Meta, aggregateId: string, aggregateVersion: number, type: string, payload: object) {
  await client.query(
    `insert into events (event_id, event_type, version, aggregate_id, aggregate_version, occurred_at,
       correlation_id, causation_id, issued_by, payload)
     values ($1, $2, 1, $3, $4, $5, $6, $7, $8, $9)`,
    [randomUUID(), type, aggregateId, aggregateVersion, meta.occurredAt, meta.correlationId, meta.causationId, meta.issuedBy, payload],
  );
}

export async function bumpAccount(client: pg.PoolClient, accountId: string) {
  const { rows } = await client.query("update accounts set version = version + 1 where account_id = $1 returning version, academic_year_id", [accountId]);
  if (!rows[0]) throw new Refused(404, "The account does not exist.");
  return rows[0] as { version: number; academic_year_id: string };
}

const earlier = async <T>(client: pg.Pool | pg.PoolClient, commandId: string): Promise<Result<T> | undefined> => {
  const { rows } = await client.query("select result from handled where message_id = $1", [commandId]);
  return rows[0] ? { value: rows[0].result, replayed: true } : undefined;
};

export async function runCommand<P, T extends object>(
  pool: pg.Pool,
  command: Command<P>,
  work: (client: pg.PoolClient, meta: Meta) => Promise<T>,
  now = new Date(),
): Promise<Result<T>> {
  const meta = { correlationId: command.correlationId, causationId: command.commandId, issuedBy: command.issuedBy, occurredAt: now };
  try {
    return await transaction(pool, async (client) => {
      const replay = await earlier<T>(client, command.commandId);
      if (replay) return replay;
      const value = await work(client, meta);
      await client.query("insert into handled (message_id, result) values ($1, $2)", [command.commandId, value]);
      return { value, replayed: false };
    });
  } catch (error) {
    if ((error as { constraint?: string }).constraint === "handled_pkey") {
      const replay = await earlier<T>(pool, command.commandId);
      if (replay) return replay;
    }
    throw error;
  }
}
