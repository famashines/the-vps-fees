import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { headers } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import type { EventEnvelopeV1 } from "@famashines/the-vps-contracts";
import pg from "pg";
import { buildApp } from "./app.ts";
import { createPool, migrate } from "./db.ts";
import { chargeAll, getAccount, monthsBetween, receivePayment, reverseCharge, reversePayment, setFeeSchedule } from "./fees.ts";
import { connectNats } from "./relay.ts";
import { handleEvent, startSaga } from "./saga.ts";

const base = process.env.TEST_DATABASE_URL;
const natsUrl = process.env.TEST_NATS_URL;
const skip = !base && "TEST_DATABASE_URL is not set";
const schema = `test_${randomUUID().replaceAll("-", "")}`;
let pool: pg.Pool;

before(async () => {
  if (!base) return;
  const admin = new pg.Client({ connectionString: base });
  await admin.connect();
  await admin.query(`create schema ${schema}`);
  await admin.end();
  const url = new URL(base);
  url.searchParams.set("options", `-c search_path=${schema}`);
  pool = createPool(url.href);
  await migrate(pool);
  await migrate(pool);
});

after(async () => {
  if (!base) return;
  await pool.query(`drop schema ${schema} cascade`);
  await pool.end();
});

const at = (iso: string) => new Date(iso);
const rs = (rupees: number) => ({ amount: rupees * 100, currency: "PKR" as const });

const event = (eventType: string, payload: object, occurredAt = "2026-10-25T07:00:00Z", eventId: string = randomUUID()): EventEnvelopeV1 => ({
  eventId,
  eventType,
  version: 1,
  occurredAt,
  aggregateId: randomUUID(),
  aggregateVersion: 1,
  correlationId: randomUUID(),
  causationId: randomUUID(),
  payload,
});

const command = <P>(commandType: string, payload: P, commandId: string = randomUUID()) => ({
  commandId,
  commandType,
  version: 1,
  issuedAt: new Date().toISOString(),
  issuedBy: randomUUID(),
  correlationId: randomUUID(),
  payload,
});

async function schoolYear(name: string, startsOn: string, endsOn: string, grade = "class-3") {
  const academicYearId = randomUUID();
  const classSectionId = randomUUID();
  await handleEvent(pool, event("admissions.AcademicYearOpened", { academicYearId, name, startsOn, endsOn }));
  await handleEvent(pool, event("admissions.ClassSectionCreated", { classSectionId, academicYearId, grade, section: "A" }));
  return { academicYearId, classSectionId };
}

async function enroll(year: { academicYearId: string; classSectionId: string }, occurredAt: string, now = at(occurredAt), eventId = randomUUID()) {
  const enrollmentId = randomUUID();
  const e = event("admissions.StudentEnrolled", { enrollmentId, studentId: randomUUID(), ...year }, occurredAt, eventId);
  const handled = await handleEvent(pool, e, now);
  const { rows } = await pool.query("select account_id from accounts where enrollment_id = $1", [enrollmentId]);
  return { e, handled, enrollmentId, accountId: rows[0]?.account_id as string };
}

const account = async (accountId: string) => (await getAccount(pool, accountId))!;
const kinds = async (accountId: string) => (await account(accountId)).charges.map((c) => c.month ?? c.kind);

test("the months between two months, over the new year", () => {
  assert.deepEqual(monthsBetween("2026-11", "2027-02"), ["2026-11", "2026-12", "2027-01", "2027-02"]);
  assert.deepEqual(monthsBetween("2026-11", "2026-11"), ["2026-11"]);
  assert.deepEqual(monthsBetween("2026-12", "2026-11"), []);
});

test("saga: an enrollment before the schedule waits; the schedule charges the one-time fees and the due months once", { skip }, async () => {
  const year = await schoolYear("2026-27", "2026-04-01", "2027-03-31");
  const first = await enroll(year, "2026-10-25T07:00:00Z");
  assert.equal(first.handled, true);
  assert.deepEqual(await kinds(first.accountId), []);
  assert.equal(await handleEvent(pool, first.e), false);
  assert.equal((await pool.query("select count(*)::int as n from accounts where enrollment_id = $1", [first.enrollmentId])).rows[0].n, 1);

  const set = await setFeeSchedule(pool, command("fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500), admissionFee: rs(10000), annualFee: rs(6000) }), at("2026-10-26T05:00:00Z"));
  assert.equal(set.value.charged, 3);
  const opened = await account(first.accountId);
  assert.deepEqual(opened.charges.map((c) => [c.month ?? c.kind, c.amount.amount, c.dueOn]), [
    ["2026-10", 450000, "2026-10-25"],
    ["admission", 1000000, "2026-10-26"],
    ["annual", 600000, "2026-10-26"],
  ]);
  assert.deepEqual(opened.balance, rs(20500));

  assert.equal(await chargeAll(pool, at("2026-12-05T05:00:00Z")), 2);
  assert.equal(await chargeAll(pool, at("2026-12-05T05:00:00Z")), 0);
  assert.equal(await chargeAll(pool, at("2027-06-01T05:00:00Z")), 3);
  assert.deepEqual(await kinds(first.accountId), ["2026-10", "admission", "annual", "2026-11", "2026-12", "2027-01", "2027-02", "2027-03"]);

  const second = await enroll(year, "2026-11-03T07:00:00Z");
  assert.deepEqual(await kinds(second.accountId), ["admission", "annual", "2026-11"]);
  assert.equal((await account(second.accountId)).charges[2].dueOn, "2026-11-10");

  const { rows } = await pool.query("select event_type, aggregate_version from events where aggregate_id = $1 order by aggregate_version", [second.accountId]);
  assert.deepEqual(rows.map((r) => `${r.aggregate_version} ${r.event_type}`), ["1 fees.StudentAccountOpened", "2 fees.FeeCharged", "3 fees.FeeCharged", "4 fees.FeeCharged"]);
});

test("saga: a section change moves the account to the grade of the new section", { skip }, async () => {
  const year = await schoolYear("2027-28", "2027-04-01", "2028-03-31");
  const other = randomUUID();
  await handleEvent(pool, event("admissions.ClassSectionCreated", { classSectionId: other, academicYearId: year.academicYearId, grade: "class-4", section: "B" }));
  const { enrollmentId, accountId } = await enroll(year, "2027-05-02T07:00:00Z");
  await handleEvent(pool, event("admissions.StudentSectionChanged", { enrollmentId, studentId: randomUUID(), academicYearId: year.academicYearId, classSectionId: other, previousClassSectionId: year.classSectionId }));
  assert.equal((await account(accountId)).grade, "class-4");
});

test("payments: at the same time they get receipt numbers with no gaps, and lower the balance", { skip }, async () => {
  const year = await schoolYear("2030-31", "2030-04-01", "2031-03-31");
  await setFeeSchedule(pool, command("fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500) }));
  const { accountId } = await enroll(year, "2030-04-02T07:00:00Z", at("2030-04-02T07:00:00Z"));
  assert.deepEqual((await account(accountId)).balance, rs(4500));

  const pay = (rupees: number, id?: string) => receivePayment(pool, command("fees.ReceivePayment", { accountId, amount: rs(rupees), method: "cash" as const }, id));
  const paid = await Promise.all([1, 2, 3, 4, 5].map(() => pay(1000)));
  assert.deepEqual(paid.map((p) => p.value.receiptNumber).sort(), ["R-2030-31-000001", "R-2030-31-000002", "R-2030-31-000003", "R-2030-31-000004", "R-2030-31-000005"]);
  assert.deepEqual((await account(accountId)).balance, rs(-500));

  await assert.rejects(receivePayment(pool, command("fees.ReceivePayment", { accountId: randomUUID(), amount: rs(10), method: "cash" as const })), { status: 404 });
  const id = randomUUID();
  const next = await pay(50, id);
  assert.equal(next.value.receiptNumber, "R-2030-31-000006");
  const again = await pay(50, id);
  assert.equal(again.replayed, true);
  assert.equal(again.value.receiptNumber, "R-2030-31-000006");
  assert.equal((await account(accountId)).payments.length, 6);
});

test("reversals: once each, with the full amount, and the receipt number stays", { skip }, async () => {
  const year = await schoolYear("2031-32", "2031-04-01", "2032-03-31");
  await setFeeSchedule(pool, command("fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500), admissionFee: rs(10000) }));
  const { accountId } = await enroll(year, "2031-04-02T07:00:00Z");
  const payment = (await receivePayment(pool, command("fees.ReceivePayment", { accountId, amount: rs(4500), method: "cash" as const }))).value;
  assert.deepEqual((await account(accountId)).balance, rs(10000));

  const undo = await reversePayment(pool, command("fees.ReversePayment", { paymentId: payment.paymentId, reason: "Entered twice" }));
  assert.equal(undo.value.receiptNumber, payment.receiptNumber);
  assert.deepEqual((await account(accountId)).balance, rs(14500));
  await assert.rejects(reversePayment(pool, command("fees.ReversePayment", { paymentId: payment.paymentId, reason: "Again" })), { status: 409 });
  await assert.rejects(reversePayment(pool, command("fees.ReversePayment", { paymentId: randomUUID(), reason: "None" })), { status: 404 });

  const admission = (await account(accountId)).charges.find((c) => c.kind === "admission")!;
  await reverseCharge(pool, command("fees.ReverseCharge", { chargeId: admission.chargeId, reason: "Sibling: no admission fee" }));
  await assert.rejects(reverseCharge(pool, command("fees.ReverseCharge", { chargeId: admission.chargeId, reason: "Again" })), { status: 409 });
  const after = await account(accountId);
  assert.deepEqual(after.balance, rs(4500));
  assert.equal(after.charges.find((c) => c.kind === "admission")!.reversal!.reason, "Sibling: no admission fee");
  assert.equal(after.payments[0].reversal!.reason, "Entered twice");
  await chargeAll(pool, at("2031-04-20T07:00:00Z"));
  assert.equal((await account(accountId)).charges.filter((c) => c.kind === "admission").length, 1);
});

test("http: commands are checked, an unknown year or account gives 404, the account has its ledger", { skip }, async () => {
  const app = buildApp(pool);
  const post = (path: string, type: string, payload: object) => app.inject({ method: "POST", url: `/commands/${path}`, payload: command(type, payload) });
  const year = await schoolYear("2032-33", "2032-04-01", "2033-03-31");
  assert.equal((await post("set-fee-schedule", "fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: { amount: 4500.5, currency: "PKR" } })).statusCode, 400);
  assert.equal((await post("set-fee-schedule", "fees.SetFeeSchedule", { academicYearId: randomUUID(), grade: "class-3", monthlyFee: rs(4500) })).statusCode, 404);
  assert.equal((await post("set-fee-schedule", "fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500) })).statusCode, 201);
  const { accountId } = await enroll(year, "2032-04-02T07:00:00Z");
  assert.equal((await post("receive-payment", "fees.ReceivePayment", { accountId, amount: { amount: 99, currency: "PKR" }, method: "cash" })).statusCode, 400);
  assert.equal((await post("receive-payment", "fees.ReceivePayment", { accountId, amount: rs(100), method: "card" })).statusCode, 400);
  assert.equal((await post("receive-payment", "fees.ReceivePayment", { accountId, amount: rs(100), method: "cash" })).statusCode, 201);
  assert.equal((await post("reverse-charge", "fees.ReverseCharge", { chargeId: randomUUID(), reason: "" })).statusCode, 400);
  assert.equal((await app.inject({ url: "/accounts/x" })).statusCode, 404);
  assert.equal((await app.inject({ url: `/accounts/${randomUUID()}` })).statusCode, 404);
  const one = (await app.inject({ url: `/accounts/${accountId}` })).json();
  assert.equal(one.academicYearName, "2032-33");
  assert.equal(one.payments[0].receiptNumber, "R-2032-33-000001");
  assert.ok((await app.inject({ url: "/accounts" })).json().some((a: { accountId: string }) => a.accountId === accountId));
  assert.ok((await app.inject({ url: "/fee-schedules" })).json().some((s: { academicYearId: string }) => s.academicYearId === year.academicYearId));
});

test("saga over NATS: each event once in stream order, also when it comes twice", { skip: (!base || !natsUrl) && "TEST_DATABASE_URL and TEST_NATS_URL are needed" }, async () => {
  const nc = await connectNats(natsUrl!);
  const id = randomUUID().slice(0, 8);
  const stream = `TEST_${id}`;
  await (await jetstreamManager(nc)).streams.add({ name: stream, subjects: [`t${id}.>`], storage: "memory" });
  const js = jetstream(nc);
  const academicYearId = randomUUID();
  const classSectionId = randomUUID();
  const enrollmentId = randomUUID();
  const enrolled = event("admissions.StudentEnrolled", { enrollmentId, studentId: randomUUID(), academicYearId, classSectionId });
  for (const e of [
    event("admissions.AcademicYearOpened", { academicYearId, name: "2033-34", startsOn: "2033-04-01", endsOn: "2034-03-31" }),
    event("admissions.ClassSectionCreated", { classSectionId, academicYearId, grade: "class-3", section: "A" }),
    enrolled,
    enrolled,
  ]) {
    const h = headers();
    h.set("Nats-Msg-Id", randomUUID());
    await js.publish(`t${id}.${e.eventType}`, JSON.stringify(e), { headers: h });
  }
  const stop = await startSaga(pool, nc, { info: () => {}, warn: () => {} }, stream);
  let n = 0;
  for (let i = 0; i < 50 && n === 0; i++) {
    await new Promise((resolve) => setTimeout(resolve, 100));
    n = (await pool.query("select count(*)::int as n from accounts where enrollment_id = $1", [enrollmentId])).rows[0].n;
  }
  await new Promise((resolve) => setTimeout(resolve, 500));
  assert.equal((await pool.query("select count(*)::int as n from accounts where enrollment_id = $1", [enrollmentId])).rows[0].n, 1);
  stop();
  await (await jetstreamManager(nc)).streams.delete(stream);
  await nc.close();
});
