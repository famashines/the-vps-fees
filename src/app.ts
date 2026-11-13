import Fastify, { type FastifyServerOptions } from "fastify";
import type pg from "pg";
import { Refused, type Command, type Result, zeroCommandCounts } from "./command.ts";
import { cashFlow, listExpenses, recordExpense, reverseExpense } from "./expenses.ts";
import { getAccount, listAccounts, listSchedules, receivePayment, reverseCharge, reversePayment, setFeeSchedule } from "./fees.ts";
import { errorsOf, payloadValidators, validEnvelope } from "./validate.ts";

type CommandType = keyof typeof payloadValidators;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function buildApp(pool: pg.Pool, options: FastifyServerOptions = {}) {
  const app = Fastify(options);
  const release = process.env.RELEASE ?? "local";

  app.get("/health", { logLevel: "warn" }, async () => ({ status: "ok", release }));

  app.get("/ready", { logLevel: "warn" }, async (_request, reply) => {
    try {
      await pool.query("select 1");
      return { status: "ok" };
    } catch {
      return reply.code(503).send({ status: "database unavailable" });
    }
  });

  const handlers: Record<CommandType, (command: Command<never>) => Promise<Result<object>>> = {
    "fees.SetFeeSchedule": (c) => setFeeSchedule(pool, c),
    "fees.ReceivePayment": (c) => receivePayment(pool, c),
    "fees.ReverseCharge": (c) => reverseCharge(pool, c),
    "fees.ReversePayment": (c) => reversePayment(pool, c),
    "fees.RecordExpense": (c) => recordExpense(pool, c),
    "fees.ReverseExpense": (c) => reverseExpense(pool, c),
  };
  zeroCommandCounts(Object.keys(handlers));

  const route = (path: string, type: CommandType) =>
    app.post(path, async (request, reply) => {
      const command = request.body as Command<never>;
      if (!validEnvelope(command)) return reply.code(400).send({ error: errorsOf(validEnvelope) });
      if (command.commandType !== type || command.version !== 1) {
        return reply.code(400).send({ error: `commandType must be ${type}, version 1` });
      }
      const validPayload = payloadValidators[type];
      if (!validPayload(command.payload)) return reply.code(400).send({ error: errorsOf(validPayload) });
      try {
        const { value, replayed } = await handlers[type](command);
        request.log.info({ commandType: type, commandId: command.commandId, replayed }, "command done");
        return reply.code(replayed ? 200 : 201).send(value);
      } catch (error) {
        if (error instanceof Refused) {
          const fields = error.field ? [{ field: error.field, message: error.message }] : undefined;
          return reply.code(error.status).send({ error: error.message, fields });
        }
        throw error;
      }
    });

  route("/commands/set-fee-schedule", "fees.SetFeeSchedule");
  route("/commands/receive-payment", "fees.ReceivePayment");
  route("/commands/reverse-charge", "fees.ReverseCharge");
  route("/commands/reverse-payment", "fees.ReversePayment");
  route("/commands/record-expense", "fees.RecordExpense");
  route("/commands/reverse-expense", "fees.ReverseExpense");

  app.get("/fee-schedules", async () => listSchedules(pool));
  app.get("/accounts", async () => listAccounts(pool));
  app.get<{ Params: { accountId: string } }>("/accounts/:accountId", async (request, reply) => {
    const account = uuid.test(request.params.accountId) && (await getAccount(pool, request.params.accountId));
    return account || reply.code(404).send({ error: "The account does not exist." });
  });

  const day = /^\d{4}-\d{2}-\d{2}$/;
  const period = (path: string, read: (from: string, to: string) => Promise<object>) =>
    app.get<{ Querystring: { from?: string; to?: string } }>(path, async (request, reply) => {
      const { from = "", to = "" } = request.query;
      const days = (Date.parse(to) - Date.parse(from)) / 86_400_000;
      if (!day.test(from) || !day.test(to) || !(days >= 0 && days <= 800)) {
        return reply.code(400).send({ error: "from and to must be days (YYYY-MM-DD), from not after to, and at most 800 days apart." });
      }
      return read(from, to);
    });
  period("/expenses", (from, to) => listExpenses(pool, from, to));
  period("/cash-flow", (from, to) => cashFlow(pool, from, to));

  app.get<{ Querystring: { limit?: string } }>("/events", async (request) => {
    const limit = Math.min(Math.max(Number(request.query.limit) || 50, 1), 100);
    const { rows } = await pool.query("select event_id, event_type, occurred_at, aggregate_id, issued_by, payload from events order by occurred_at desc, aggregate_version desc limit $1", [limit]);
    return rows.map((r) => ({ eventId: r.event_id, eventType: r.event_type, occurredAt: r.occurred_at.toISOString(), aggregateId: r.aggregate_id, issuedBy: r.issued_by, payload: r.payload }));
  });

  return app;
}
