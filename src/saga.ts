import { randomUUID } from "node:crypto";
import type {
  AcademicYearOpenedEventPayloadV1,
  ClassOpenedEventPayloadV1,
  EventEnvelopeV1,
  StudentAccountOpenedEventPayloadV1,
  StudentClassChangedEventPayloadV1,
  StudentEnrolledEventPayloadV1,
  StudentLeavingCancelledEventPayloadV1,
  StudentLeftEventPayloadV1,
} from "@famashines/the-vps-contracts";
import { AckPolicy, DeliverPolicy, jetstream, jetstreamManager } from "@nats-io/jetstream";
import { metrics, propagation, ROOT_CONTEXT, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import type { NatsConnection } from "@nats-io/transport-node";
import type pg from "pg";
import { append, transaction, type Meta } from "./command.ts";
import { chargeDue, leave, todayInPakistan } from "./fees.ts";
import type { RelayLog } from "./relay.ts";

type Old = { classSectionId?: string };
const classOf = (p: { classId: string } & Old) => p.classId ?? p.classSectionId;

async function openAccount(client: pg.PoolClient, event: EventEnvelopeV1, meta: Meta) {
  const p = event.payload as StudentEnrolledEventPayloadV1 & Old;
  const classId = classOf(p);
  const { rows } = await client.query("select grade from classes where class_id = $1", [classId]);
  if (!rows[0]) throw new Error(`class ${classId} is not known yet`);
  const account: StudentAccountOpenedEventPayloadV1 = {
    accountId: randomUUID(),
    studentId: p.studentId,
    enrollmentId: p.enrollmentId,
    academicYearId: p.academicYearId,
    classId,
    grade: rows[0].grade,
    openedOn: todayInPakistan(new Date(event.occurredAt)),
  };
  await client.query(
    `insert into accounts (account_id, student_id, enrollment_id, academic_year_id, class_id, grade, opened_on, version)
     values ($1, $2, $3, $4, $5, $6, $7, 1)`,
    [account.accountId, p.studentId, p.enrollmentId, p.academicYearId, classId, account.grade, account.openedOn],
  );
  await append(client, meta, account.accountId, 1, "fees.StudentAccountOpened", account);
  await chargeDue(client, account.accountId, meta);
}

const tracer = trace.getTracer("the-vps-fees");
const meter = metrics.getMeter("the-vps-fees");
const handled = meter.createCounter("events_handled", { description: "Events read by the saga, by type and result" });

export async function handleEvent(pool: pg.Pool, event: EventEnvelopeV1, now = new Date()) {
  return transaction(pool, async (client) => {
    const { rowCount } = await client.query("insert into handled (message_id, result) values ($1, '{}') on conflict do nothing", [event.eventId]);
    if (!rowCount) return false;
    const meta = { correlationId: event.correlationId, causationId: event.eventId, issuedBy: null, occurredAt: now };
    if (event.eventType === "admissions.AcademicYearOpened" || event.eventType === "admissions.AcademicYearUpdated") {
      const y = event.payload as AcademicYearOpenedEventPayloadV1;
      await client.query(
        `insert into years (academic_year_id, name, starts_on, ends_on) values ($1, $2, $3, $4)
         on conflict (academic_year_id) do update set name = excluded.name, starts_on = excluded.starts_on, ends_on = excluded.ends_on`,
        [y.academicYearId, y.name, y.startsOn, y.endsOn],
      );
    } else if (["admissions.ClassOpened", "admissions.ClassSectionCreated", "admissions.ClassSectionUpdated"].includes(event.eventType)) {
      const c = event.payload as ClassOpenedEventPayloadV1 & Old;
      await client.query(
        `insert into classes (class_id, academic_year_id, grade) values ($1, $2, $3)
         on conflict (class_id) do update set grade = excluded.grade`,
        [classOf(c), c.academicYearId, c.grade],
      );
    } else if (event.eventType === "admissions.ClassClosed") {
      await client.query("delete from classes where class_id = $1", [(event.payload as ClassOpenedEventPayloadV1).classId]);
    } else if (event.eventType === "admissions.StudentEnrolled") {
      await openAccount(client, event, meta);
    } else if (event.eventType === "admissions.StudentClassChanged" || event.eventType === "admissions.StudentSectionChanged") {
      const c = event.payload as StudentClassChangedEventPayloadV1 & Old;
      await client.query(
        "update accounts set class_id = $2, grade = (select grade from classes where class_id = $2) where enrollment_id = $1",
        [c.enrollmentId, classOf(c)],
      );
    } else if (event.eventType === "admissions.StudentLeft") {
      const l = event.payload as StudentLeftEventPayloadV1;
      await leave(client, l.studentId, l.leftOn, meta);
    } else if (event.eventType === "admissions.StudentLeavingCancelled") {
      const { studentId } = event.payload as StudentLeavingCancelledEventPayloadV1;
      const { rows } = await client.query("update accounts set left_on = null where student_id = $1 returning account_id", [studentId]);
      for (const r of rows) await chargeDue(client, r.account_id, meta);
    }
    return true;
  });
}

export async function startSaga(pool: pg.Pool, nc: NatsConnection, log: RelayLog, stream = "ADMISSIONS", durable = "fees") {
  const jsm = await jetstreamManager(nc);
  await jsm.consumers.info(stream, durable).catch(() =>
    jsm.consumers.add(stream, { durable_name: durable, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, max_ack_pending: 1 }),
  );
  meter.createObservableGauge("saga_pending_events", { description: "Events in the stream that the saga has not read yet" }).addCallback(async (result) => {
    const info = await jsm.consumers.info(stream, durable);
    result.observe(info.num_pending + info.num_ack_pending, { stream, consumer: durable });
  });
  const messages = await (await jetstream(nc).consumers.get(stream, durable)).consume({ max_messages: 1 });
  void (async () => {
    for await (const m of messages) {
      const event = m.json<EventEnvelopeV1>();
      const traceparent = m.headers?.get("traceparent");
      const parent = traceparent ? propagation.extract(ROOT_CONTEXT, { traceparent }) : ROOT_CONTEXT;
      await tracer.startActiveSpan(`process ${event.eventType}`, {
        kind: SpanKind.CONSUMER,
        attributes: { "messaging.system": "nats", "messaging.operation.type": "process", "messaging.destination.name": m.subject, "messaging.message.id": event.eventId },
      }, parent, async (span) => {
        try {
          const done = await handleEvent(pool, event);
          if (done) log.info({ eventType: event.eventType, eventId: event.eventId }, "event handled");
          handled.add(1, { event_type: event.eventType, result: done ? "handled" : "duplicate" });
          m.ack();
        } catch (error) {
          span.setStatus({ code: SpanStatusCode.ERROR });
          span.recordException(error as Error);
          handled.add(1, { event_type: event.eventType, result: "failed" });
          log.warn({ err: error, eventType: event.eventType, eventId: event.eventId }, "event failed; it comes again in 5 s");
          m.nak(5000);
        } finally {
          span.end();
        }
      });
    }
  })();
  return () => messages.stop();
}
