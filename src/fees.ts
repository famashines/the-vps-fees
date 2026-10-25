import { randomUUID } from "node:crypto";
import type {
  ChargeReversedEventPayloadV1,
  FeeChargedEventPayloadV1,
  FeeScheduleSetEventPayloadV1,
  PaymentReceivedEventPayloadV1,
  PaymentReversedEventPayloadV1,
  ReceivePaymentCommandPayloadV1,
  ReverseChargeCommandPayloadV1,
  ReversePaymentCommandPayloadV1,
  SetFeeScheduleCommandPayloadV1,
} from "@famashines/the-vps-contracts";
import type pg from "pg";
import { append, bumpAccount, Refused, runCommand, transaction, type Command, type Meta } from "./command.ts";

export const rs = (amount: number) => ({ amount, currency: "PKR" as const });
export const todayInPakistan = (date: Date) => new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Karachi" }).format(date);
const later = (a: string, b: string) => (a > b ? a : b);
const earlier = (a: string, b: string) => (a < b ? a : b);

export function monthsBetween(from: string, to: string) {
  const months: string[] = [];
  let [y, m] = from.split("-").map(Number);
  for (let month = from; month <= to; month = `${y}-${String(m).padStart(2, "0")}`) {
    months.push(month);
    [y, m] = m === 12 ? [y + 1, 1] : [y, m + 1];
  }
  return months;
}

export async function chargeDue(client: pg.PoolClient, accountId: string, meta: Meta) {
  const today = todayInPakistan(meta.occurredAt);
  const { rows } = await client.query(
    `select a.opened_on, a.one_time_charged, a.version, y.starts_on, y.ends_on,
       f.fee_schedule_id, f.monthly_fee, f.admission_fee, f.annual_fee
     from accounts a join years y using (academic_year_id)
     left join fee_schedules f using (academic_year_id, grade)
     where a.account_id = $1 for update of a`,
    [accountId],
  );
  const a = rows[0];
  if (!a?.fee_schedule_id) return 0;
  const due: [FeeChargedEventPayloadV1["kind"], string | undefined, number, string][] = [];
  if (!a.one_time_charged) {
    due.push(["admission", undefined, a.admission_fee ?? 0, today], ["annual", undefined, a.annual_fee ?? 0, today]);
  }
  for (const month of monthsBetween(later(a.starts_on, a.opened_on).slice(0, 7), earlier(a.ends_on, today).slice(0, 7))) {
    due.push(["monthly", month, a.monthly_fee, later(`${month}-10`, a.opened_on)]);
  }
  let version = a.version;
  for (const [kind, month, amount, dueOn] of due.filter((d) => d[2] > 0)) {
    const chargeId = randomUUID();
    const { rowCount } = await client.query(
      `insert into charges (charge_id, account_id, kind, month, amount, due_on, charged_at)
       values ($1, $2, $3, $4, $5, $6, $7) on conflict do nothing`,
      [chargeId, accountId, kind, month ?? null, amount, dueOn, meta.occurredAt],
    );
    if (!rowCount) continue;
    const charge: FeeChargedEventPayloadV1 = { chargeId, accountId, kind, ...(month && { month }), amount: rs(amount), dueOn };
    await append(client, meta, accountId, ++version, "fees.FeeCharged", charge);
  }
  await client.query("update accounts set version = $2, one_time_charged = true where account_id = $1", [accountId, version]);
  return version - a.version;
}

export async function chargeAll(pool: pg.Pool, now = new Date()) {
  const run = randomUUID();
  const meta = { correlationId: run, causationId: run, issuedBy: null, occurredAt: now };
  const { rows } = await pool.query("select account_id from accounts join fee_schedules using (academic_year_id, grade)");
  let charged = 0;
  for (const r of rows) charged += await transaction(pool, (client) => chargeDue(client, r.account_id, meta));
  return charged;
}

export async function setFeeSchedule(pool: pg.Pool, command: Command<SetFeeScheduleCommandPayloadV1>, now = new Date()) {
  const p = command.payload;
  return runCommand(pool, command, async (client, meta) => {
    const { rows } = await client.query(
      `insert into fee_schedules (fee_schedule_id, academic_year_id, grade, monthly_fee, admission_fee, annual_fee, version)
       values ($1, $2, $3, $4, $5, $6, 1)
       on conflict (academic_year_id, grade) do update set monthly_fee = excluded.monthly_fee,
         admission_fee = excluded.admission_fee, annual_fee = excluded.annual_fee, version = fee_schedules.version + 1
       returning fee_schedule_id, version`,
      [randomUUID(), p.academicYearId, p.grade, p.monthlyFee.amount, p.admissionFee?.amount ?? null, p.annualFee?.amount ?? null],
    );
    const schedule: FeeScheduleSetEventPayloadV1 = { feeScheduleId: rows[0].fee_schedule_id, ...p };
    await append(client, meta, schedule.feeScheduleId, rows[0].version, "fees.FeeScheduleSet", schedule);
    const accounts = await client.query("select account_id from accounts where academic_year_id = $1 and grade = $2", [p.academicYearId, p.grade]);
    let charged = 0;
    for (const r of accounts.rows) charged += await chargeDue(client, r.account_id, meta);
    return { ...schedule, charged };
  }, now);
}

export async function receivePayment(pool: pg.Pool, command: Command<ReceivePaymentCommandPayloadV1>, now = new Date()) {
  const { accountId, amount, method } = command.payload;
  return runCommand(pool, command, async (client, meta) => {
    const account = await bumpAccount(client, accountId);
    const { rows } = await client.query(
      `with counter as (
         insert into receipt_counters (academic_year_id, last) values ($1, 1)
         on conflict (academic_year_id) do update set last = receipt_counters.last + 1 returning last)
       select counter.last, years.name from counter, years where years.academic_year_id = $1`,
      [account.academic_year_id],
    );
    const payment: PaymentReceivedEventPayloadV1 = {
      paymentId: randomUUID(),
      accountId,
      receiptNumber: `R-${rows[0].name}-${String(rows[0].last).padStart(6, "0")}`,
      amount,
      method,
    };
    await client.query(
      `insert into payments (payment_id, account_id, receipt_number, amount, method, received_at, received_by)
       values ($1, $2, $3, $4, $5, $6, $7)`,
      [payment.paymentId, accountId, payment.receiptNumber, amount.amount, method, now, command.issuedBy],
    );
    await append(client, meta, accountId, account.version, "fees.PaymentReceived", payment);
    return payment;
  }, now);
}

async function reverse(client: pg.PoolClient, meta: Meta, table: "charges" | "payments", key: string, id: string, reason: string, what: string) {
  const found = await client.query(`select account_id from ${table} where ${key} = $1`, [id]);
  if (!found.rows[0]) throw new Refused(404, `The ${what} does not exist.`);
  const account = await bumpAccount(client, found.rows[0].account_id);
  const { rows } = await client.query(
    `update ${table} set reversed_at = $2, reversal_reason = $3, reversed_by = $4 where ${key} = $1 and reversed_at is null returning *`,
    [id, meta.occurredAt, reason, meta.issuedBy],
  );
  if (!rows[0]) throw new Refused(409, `This ${what} is already reversed.`);
  return { row: rows[0], version: account.version };
}

export async function reverseCharge(pool: pg.Pool, command: Command<ReverseChargeCommandPayloadV1>, now = new Date()) {
  const { chargeId, reason } = command.payload;
  return runCommand(pool, command, async (client, meta) => {
    const { row, version } = await reverse(client, meta, "charges", "charge_id", chargeId, reason, "charge");
    const reversal: ChargeReversedEventPayloadV1 = { chargeId, accountId: row.account_id, amount: rs(row.amount), reason };
    await append(client, meta, row.account_id, version, "fees.ChargeReversed", reversal);
    return reversal;
  }, now);
}

export async function reversePayment(pool: pg.Pool, command: Command<ReversePaymentCommandPayloadV1>, now = new Date()) {
  const { paymentId, reason } = command.payload;
  return runCommand(pool, command, async (client, meta) => {
    const { row, version } = await reverse(client, meta, "payments", "payment_id", paymentId, reason, "payment");
    const reversal: PaymentReversedEventPayloadV1 = { paymentId, accountId: row.account_id, receiptNumber: row.receipt_number, amount: rs(row.amount), reason };
    await append(client, meta, row.account_id, version, "fees.PaymentReversed", reversal);
    return reversal;
  }, now);
}

const optional = (amount: number | null) => (amount === null ? undefined : rs(amount));

export async function listSchedules(pool: pg.Pool) {
  const { rows } = await pool.query("select * from fee_schedules order by academic_year_id, grade");
  return rows.map((r) => ({
    feeScheduleId: r.fee_schedule_id,
    version: r.version,
    academicYearId: r.academic_year_id,
    grade: r.grade,
    monthlyFee: rs(r.monthly_fee),
    admissionFee: optional(r.admission_fee),
    annualFee: optional(r.annual_fee),
  }));
}

const accountSql = `
  select a.*, y.name as academic_year_name,
    coalesce((select sum(amount) from charges c where c.account_id = a.account_id and c.reversed_at is null), 0)::bigint as charged,
    coalesce((select sum(amount) from payments p where p.account_id = a.account_id and p.reversed_at is null), 0)::bigint as paid
  from accounts a join years y using (academic_year_id)`;

const accountOf = (r: Record<string, any>) => ({
  accountId: r.account_id,
  studentId: r.student_id,
  enrollmentId: r.enrollment_id,
  academicYearId: r.academic_year_id,
  academicYearName: r.academic_year_name,
  classSectionId: r.class_section_id,
  grade: r.grade,
  openedOn: r.opened_on,
  charged: rs(r.charged),
  paid: rs(r.paid),
  balance: rs(r.charged - r.paid),
});

export async function listAccounts(pool: pg.Pool) {
  const { rows } = await pool.query(`${accountSql} order by y.starts_on desc, a.opened_on desc`);
  return rows.map(accountOf);
}

const reversalOf = (r: Record<string, any>) => (r.reversed_at ? { reversedAt: r.reversed_at.toISOString(), reason: r.reversal_reason } : undefined);

export async function getAccount(pool: pg.Pool, accountId: string) {
  const { rows } = await pool.query(`${accountSql} where a.account_id = $1`, [accountId]);
  if (!rows[0]) return undefined;
  const charges = await pool.query("select * from charges where account_id = $1 order by due_on, charged_at, kind", [accountId]);
  const payments = await pool.query("select * from payments where account_id = $1 order by received_at", [accountId]);
  return {
    ...accountOf(rows[0]),
    charges: charges.rows.map((c) => ({ chargeId: c.charge_id, kind: c.kind, month: c.month ?? undefined, amount: rs(c.amount), dueOn: c.due_on, reversal: reversalOf(c) })),
    payments: payments.rows.map((p) => ({
      paymentId: p.payment_id,
      receiptNumber: p.receipt_number,
      amount: rs(p.amount),
      method: p.method,
      receivedAt: p.received_at.toISOString(),
      receivedBy: p.received_by,
      reversal: reversalOf(p),
    })),
  };
}
