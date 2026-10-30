import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { headers } from "@nats-io/transport-node";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import type { EventEnvelopeV1 } from "@famashines/the-vps-contracts";
import pg from "pg";
import { buildApp } from "./app.ts";
import { createPool, migrate } from "./db.ts";
import { chargeAll, getAccount, listAccounts, monthsBetween, receivePayment, reverseCharge, reversePayment, setFeeSchedule } from "./fees.ts";
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
  const classId = randomUUID();
  await handleEvent(pool, event("admissions.AcademicYearOpened", { academicYearId, name, startsOn, endsOn }));
  await handleEvent(pool, event("admissions.ClassOpened", { classId, academicYearId, grade }));
  return { academicYearId, classId };
}

async function enroll(year: { academicYearId: string; classId: string }, occurredAt: string, now = at(occurredAt), eventId = randomUUID()) {
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

test("saga: a class change moves the account to the grade of the new class; a closed class goes away", { skip }, async () => {
  const year = await schoolYear("2027-28", "2027-04-01", "2028-03-31");
  const other = randomUUID();
  await handleEvent(pool, event("admissions.ClassOpened", { classId: other, academicYearId: year.academicYearId, grade: "class-4" }));
  const { enrollmentId, accountId } = await enroll(year, "2027-05-02T07:00:00Z");
  await handleEvent(pool, event("admissions.StudentClassChanged", { enrollmentId, studentId: randomUUID(), academicYearId: year.academicYearId, classId: other, previousClassId: year.classId }));
  assert.equal((await account(accountId)).grade, "class-4");
  assert.equal((await account(accountId)).classId, other);
  await handleEvent(pool, event("admissions.ClassClosed", { classId: year.classId, academicYearId: year.academicYearId, grade: "class-3" }));
  assert.equal((await pool.query("select count(*)::int as n from classes where class_id = $1", [year.classId])).rows[0].n, 0);
});

test("saga: the old event names from before v0.6.0 still open and move an account", { skip }, async () => {
  const academicYearId = randomUUID();
  const [a, b] = [randomUUID(), randomUUID()];
  await handleEvent(pool, event("admissions.AcademicYearOpened", { academicYearId, name: "2028-29", startsOn: "2028-04-01", endsOn: "2029-03-31" }));
  await handleEvent(pool, event("admissions.ClassSectionCreated", { classSectionId: a, academicYearId, grade: "class-3", section: "A" }));
  await handleEvent(pool, event("admissions.ClassSectionCreated", { classSectionId: b, academicYearId, grade: "class-3", section: "B" }));
  await handleEvent(pool, event("admissions.ClassSectionUpdated", { classSectionId: b, academicYearId, grade: "class-5", section: "B" }));
  const enrollmentId = randomUUID();
  await handleEvent(pool, event("admissions.StudentEnrolled", { enrollmentId, studentId: randomUUID(), academicYearId, classSectionId: a }));
  const { rows } = await pool.query("select account_id from accounts where enrollment_id = $1", [enrollmentId]);
  assert.equal((await account(rows[0].account_id)).classId, a);
  await handleEvent(pool, event("admissions.StudentSectionChanged", { enrollmentId, studentId: randomUUID(), academicYearId, classSectionId: b, previousClassSectionId: a }));
  assert.equal((await account(rows[0].account_id)).grade, "class-5");
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
  const classId = randomUUID();
  const enrollmentId = randomUUID();
  const enrolled = event("admissions.StudentEnrolled", { enrollmentId, studentId: randomUUID(), academicYearId, classId });
  for (const e of [
    event("admissions.AcademicYearOpened", { academicYearId, name: "2033-34", startsOn: "2033-04-01", endsOn: "2034-03-31" }),
    event("admissions.ClassOpened", { classId, academicYearId, grade: "class-3" }),
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

test("saga: leaving keeps the leaving month, reverses the later months, and a cancel charges them again", { skip }, async () => {
  const year = await schoolYear("2028-29", "2028-04-01", "2029-03-31");
  await setFeeSchedule(pool, command("fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500) }), at("2028-04-01T05:00:00Z"));
  const { accountId } = await enroll(year, "2028-05-02T07:00:00Z");
  const { student_id: studentId } = (await pool.query("select student_id from accounts where account_id = $1", [accountId])).rows[0];
  await chargeAll(pool, at("2028-10-20T05:00:00Z"));
  assert.deepEqual(await kinds(accountId), ["2028-05", "2028-06", "2028-07", "2028-08", "2028-09", "2028-10"]);

  await handleEvent(pool, event("admissions.StudentLeft", { studentId, leftOn: "2028-09-15" }), at("2028-10-29T05:00:00Z"));
  const left = await account(accountId);
  assert.deepEqual(left.charges.filter((c) => c.reversal).map((c) => [c.month, c.reversal!.reason]), [["2028-10", "The student left the school on 2028-09-15."]]);
  assert.deepEqual(left.balance, rs(4500 * 5));
  await chargeAll(pool, at("2028-12-01T05:00:00Z"));
  assert.equal((await account(accountId)).charges.length, 6);

  await handleEvent(pool, event("admissions.StudentLeavingCancelled", { studentId, leftOn: "2028-09-15" }), at("2028-12-05T05:00:00Z"));
  const back = await account(accountId);
  assert.deepEqual(back.charges.filter((c) => !c.reversal).map((c) => c.month), ["2028-05", "2028-06", "2028-07", "2028-08", "2028-09", "2028-10", "2028-11", "2028-12"]);
  assert.deepEqual(back.balance, rs(4500 * 8));

  const november = back.charges.find((c) => c.month === "2028-11")!;
  await reverseCharge(pool, command("fees.ReverseCharge", { chargeId: november.chargeId, reason: "Waived" }));
  await chargeAll(pool, at("2029-01-02T05:00:00Z"));
  assert.deepEqual((await account(accountId)).charges.filter((c) => c.month === "2028-11").map((c) => c.reversal?.reason), ["Waived"]);

  const { rows } = await pool.query("select event_type from events where aggregate_id = $1 and event_type = 'fees.ChargeReversed'", [accountId]);
  assert.equal(rows.length, 2);
});

test("overdue: the charges due before today in Pakistan, less the payments, never below 0", { skip }, async () => {
  const year = await schoolYear("2034-35", "2034-04-01", "2035-03-31");
  await setFeeSchedule(pool, command("fees.SetFeeSchedule", { academicYearId: year.academicYearId, grade: "class-3", monthlyFee: rs(4500), admissionFee: rs(10000) }), at("2034-03-20T05:00:00Z"));
  const { accountId } = await enroll(year, "2034-04-02T07:00:00Z");
  assert.deepEqual((await account(accountId)).charges.map((c) => [c.month ?? c.kind, c.dueOn]), [["admission", "2034-04-02"], ["2034-04", "2034-04-10"]]);
  const overdue = async (iso: string) => (await getAccount(pool, accountId, at(iso)))!.overdue;

  assert.deepEqual(await overdue("2034-04-02T10:00:00Z"), rs(0));
  assert.deepEqual(await overdue("2034-04-05T05:00:00Z"), rs(10000));
  assert.deepEqual(await overdue("2034-04-10T18:59:00Z"), rs(10000));
  assert.deepEqual(await overdue("2034-04-10T19:00:00Z"), rs(14500));

  await receivePayment(pool, command("fees.ReceivePayment", { accountId, amount: rs(12000), method: "cash" as const }));
  assert.deepEqual(await overdue("2034-04-11T05:00:00Z"), rs(2500));

  const admission = (await account(accountId)).charges.find((c) => c.kind === "admission")!;
  await reverseCharge(pool, command("fees.ReverseCharge", { chargeId: admission.chargeId, reason: "Sibling: no admission fee" }));
  const after = (await getAccount(pool, accountId, at("2034-04-11T05:00:00Z")))!;
  assert.deepEqual(after.balance, rs(-7500));
  assert.deepEqual(after.overdue, rs(0));
  assert.deepEqual((await listAccounts(pool, at("2034-04-11T05:00:00Z"))).find((a) => a.accountId === accountId)!.overdue, rs(0));
});
