import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { connect, headers, type NatsConnection } from "@nats-io/transport-node";
import { context, metrics, propagation, ROOT_CONTEXT, SpanKind, trace } from "@opentelemetry/api";
import type pg from "pg";

export const STREAM = "FEES";

const tracer = trace.getTracer("the-vps-fees");
const meter = metrics.getMeter("the-vps-fees");
const published = meter.createCounter("events_published", { description: "Events published to NATS by the outbox relay" });

export type RelayLog = { info: (o: object, m: string) => void; warn: (o: object, m: string) => void };

export async function connectNats(servers: string, user?: string, pass?: string) {
  return connect({ servers, user, pass, name: "the-vps-fees", maxReconnectAttempts: -1 });
}

export async function ensureStream(nc: NatsConnection) {
  const jsm = await jetstreamManager(nc);
  try {
    await jsm.streams.info(STREAM);
  } catch {
    await jsm.streams.add({ name: STREAM, subjects: ["fees.>"], storage: "file", num_replicas: 1 });
  }
  const { state } = await jsm.streams.info(STREAM, { subjects_filter: ">" });
  for (const event_type of Object.keys(state.subjects ?? {})) published.add(0, { event_type });
}

export async function publishBatch(pool: pg.Pool, nc: NatsConnection, limit = 100) {
  const js = jetstream(nc);
  const client = await pool.connect();
  try {
    await client.query("begin");
    const { rows } = await client.query(
      `select event_id, event_type, version, occurred_at, aggregate_id, aggregate_version, correlation_id, causation_id, payload, trace_parent
       from events where published_at is null
       order by occurred_at, aggregate_version
       limit $1 for update skip locked`,
      [limit],
    );
    for (const r of rows) {
      const parent = r.trace_parent ? propagation.extract(ROOT_CONTEXT, { traceparent: r.trace_parent }) : ROOT_CONTEXT;
      const span = tracer.startSpan(`publish ${r.event_type}`, {
        kind: SpanKind.PRODUCER,
        attributes: { "messaging.system": "nats", "messaging.operation.type": "send", "messaging.destination.name": r.event_type, "messaging.message.id": r.event_id },
      }, parent);
      const h = headers();
      h.set("Nats-Msg-Id", r.event_id);
      const carrier: { traceparent?: string } = {};
      propagation.inject(trace.setSpan(parent, span), carrier);
      if (carrier.traceparent) h.set("traceparent", carrier.traceparent);
      const envelope = {
        eventId: r.event_id,
        eventType: r.event_type,
        version: r.version,
        occurredAt: new Date(r.occurred_at).toISOString(),
        aggregateId: r.aggregate_id,
        aggregateVersion: r.aggregate_version,
        correlationId: r.correlation_id,
        causationId: r.causation_id,
        payload: r.payload,
      };
      await context.with(trace.setSpan(parent, span), () => js.publish(r.event_type, JSON.stringify(envelope), { headers: h })).finally(() => span.end());
      published.add(1, { event_type: r.event_type });
    }
    if (rows.length) await client.query("update events set published_at = now() where event_id = any($1)", [rows.map((r) => r.event_id)]);
    await client.query("commit");
    return rows.length;
  } catch (error) {
    await client.query("rollback");
    throw error;
  } finally {
    client.release();
  }
}

export function observeOutbox(pool: pg.Pool) {
  meter.createObservableGauge("outbox_unpublished_events", { description: "Events in the outbox that are not published yet" }).addCallback(async (result) => {
    const { rows } = await pool.query("select count(*)::int as n from events where published_at is null");
    result.observe(rows[0].n);
  });
}

export function startRelay(pool: pg.Pool, nc: NatsConnection, log: RelayLog, everyMs = 1000) {
  let busy = false;
  let failing = false;
  const timer = setInterval(async () => {
    if (busy) return;
    busy = true;
    try {
      const sent = await publishBatch(pool, nc);
      if (sent) log.info({ sent }, "events published");
      if (failing) log.info({}, "event relay works again");
      failing = false;
    } catch (error) {
      if (!failing) log.warn({ err: error }, "event relay failed; it tries again");
      failing = true;
    } finally {
      busy = false;
    }
  }, everyMs);
  return () => clearInterval(timer);
}
