import type { NatsConnection } from "@nats-io/transport-node";
import { buildApp } from "./app.ts";
import { createPool, migrate } from "./db.ts";
import { chargeAll } from "./fees.ts";
import { connectNats, ensureStream, startRelay } from "./relay.ts";
import { startSaga } from "./saga.ts";

const url = process.env.DATABASE_URL;
if (!url) throw new Error("DATABASE_URL is not set");

const pool = createPool(url);
await migrate(pool);
const app = buildApp(pool, { logger: true });

let nc: NatsConnection | undefined;
let stopRelay = () => {};
let stopSaga = () => {};

async function chargeMonthly() {
  try {
    const charged = await chargeAll(pool);
    if (charged) app.log.info({ charged }, "fees charged");
  } catch (error) {
    app.log.warn({ err: error }, "the fee charge failed; it runs again in 1 h");
  }
}
const monthly = setInterval(chargeMonthly, 60 * 60 * 1000);

async function startEvents() {
  const servers = process.env.NATS_URL;
  if (!servers) return app.log.warn("NATS_URL is not set; events stay in the outbox");
  while (!nc) {
    try {
      nc = await connectNats(servers, process.env.NATS_USER, process.env.NATS_PASSWORD);
      await ensureStream(nc);
      stopRelay = startRelay(pool, nc, app.log);
      stopSaga = await startSaga(pool, nc, app.log);
      app.log.info({ servers }, "event relay and saga started");
    } catch (error) {
      app.log.warn({ err: error }, "NATS not ready; trying again in 5 s");
      stopRelay();
      await nc?.close().catch(() => {});
      nc = undefined;
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, async () => {
    app.log.info({ signal }, "shutting down");
    stopRelay();
    stopSaga();
    clearInterval(monthly);
    await app.close();
    await nc?.drain().catch(() => {});
    await pool.end();
    process.exit(0);
  });
}

await app.listen({ host: "0.0.0.0", port: Number(process.env.PORT ?? 8080) });
void startEvents();
void chargeMonthly();
