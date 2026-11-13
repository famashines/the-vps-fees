import { randomUUID } from "node:crypto";
import type {
  ExpenseRecordedEventPayloadV1,
  ExpenseReversedEventPayloadV1,
  RecordExpenseCommandPayloadV1,
  ReverseExpenseCommandPayloadV1,
} from "@famashines/the-vps-contracts";
import type pg from "pg";
import { append, Refused, runCommand, type Command } from "./command.ts";
import { rs, todayInPakistan } from "./fees.ts";

export async function recordExpense(pool: pg.Pool, command: Command<RecordExpenseCommandPayloadV1>, now = new Date()) {
  const p = command.payload;
  if (p.spentOn > todayInPakistan(now)) throw new Refused(422, "The day of the expense cannot be in the future.", "spentOn");
  return runCommand(pool, command, async (client, meta) => {
    const expense: ExpenseRecordedEventPayloadV1 = { expenseId: randomUUID(), ...p };
    await client.query(
      `insert into expenses (expense_id, spent_on, category, amount, description, paid_to, recorded_at, recorded_by)
       values ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [expense.expenseId, p.spentOn, p.category, p.amount.amount, p.description, p.paidTo ?? null, now, command.issuedBy],
    );
    await append(client, meta, expense.expenseId, 1, "fees.ExpenseRecorded", expense);
    return expense;
  }, now);
}

export async function reverseExpense(pool: pg.Pool, command: Command<ReverseExpenseCommandPayloadV1>, now = new Date()) {
  const { expenseId, reason } = command.payload;
  return runCommand(pool, command, async (client, meta) => {
    const { rows } = await client.query(
      "update expenses set reversed_at = $2, reversal_reason = $3, reversed_by = $4 where expense_id = $1 and reversed_at is null returning amount",
      [expenseId, now, reason, command.issuedBy],
    );
    if (!rows[0]) {
      const found = await client.query("select 1 from expenses where expense_id = $1", [expenseId]);
      throw found.rows[0] ? new Refused(409, "This expense is already reversed.") : new Refused(404, "The expense does not exist.");
    }
    const reversal: ExpenseReversedEventPayloadV1 = { expenseId, amount: rs(rows[0].amount), reason };
    await append(client, meta, expenseId, 2, "fees.ExpenseReversed", reversal);
    return reversal;
  }, now);
}

export async function listExpenses(pool: pg.Pool, from: string, to: string) {
  const { rows } = await pool.query("select * from expenses where spent_on between $1 and $2 order by spent_on desc, recorded_at desc", [from, to]);
  return rows.map((r) => ({
    expenseId: r.expense_id,
    spentOn: r.spent_on,
    category: r.category,
    amount: rs(r.amount),
    description: r.description,
    paidTo: r.paid_to ?? undefined,
    recordedAt: r.recorded_at.toISOString(),
    recordedBy: r.recorded_by,
    reversal: r.reversed_at ? { reversedAt: r.reversed_at.toISOString(), reason: r.reversal_reason } : undefined,
  }));
}

export async function cashFlow(pool: pg.Pool, from: string, to: string) {
  const { rows } = await pool.query(
    `with entries (day, charged, received, refunded, spent) as (
       select due_on, amount, 0, 0, 0 from charges where reversed_at is null
       union all
       select (received_at at time zone 'Asia/Karachi')::date, 0, amount, 0, 0 from payments
       union all
       select (reversed_at at time zone 'Asia/Karachi')::date, 0, 0, amount, 0 from payments where reversed_at is not null
       union all
       select spent_on, 0, 0, 0, amount from expenses where reversed_at is null)
     select day, sum(charged)::bigint as charged, sum(received)::bigint as received, sum(refunded)::bigint as refunded, sum(spent)::bigint as spent
     from entries where day between $1 and $2 group by day order by day`,
    [from, to],
  );
  return {
    from,
    to,
    days: rows.map((r) => ({ date: r.day, charged: rs(r.charged), received: rs(r.received), refunded: rs(r.refunded), spent: rs(r.spent) })),
  };
}
