import Fastify, { type FastifyServerOptions } from "fastify";
import type pg from "pg";
import { Refused, type Command, type Result } from "./command.ts";
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
  };

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

  app.get("/fee-schedules", async () => listSchedules(pool));
  app.get("/accounts", async () => listAccounts(pool));
  app.get<{ Params: { accountId: string } }>("/accounts/:accountId", async (request, reply) => {
    const account = uuid.test(request.params.accountId) && (await getAccount(pool, request.params.accountId));
    return account || reply.code(404).send({ error: "The account does not exist." });
  });

  return app;
}
