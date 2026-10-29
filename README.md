# the-vps-fees

The fees service of the-vps: one fee account for each enrolled student and
school year, with an append-only ledger, cash payments with receipt numbers,
and reversals. It is internal: only the gateway (`the-vps-gateway`) calls it.

Fastify on Node 24 (TypeScript, run directly), PostgreSQL through `pg`, NATS
JetStream, the contracts of `@famashines/the-vps-contracts`. The rules come
from `the-vps-docs/schemas-structure.md`, "Money and the fee ledger".

## Endpoints

| Method | Path                         | Does |
| ------ | ---------------------------- | ---- |
| GET    | `/health`, `/ready`          | Liveness, and readiness with the database |
| POST   | `/commands/set-fee-schedule` | `SetFeeScheduleCommandPayloadV1`: the fees of one grade in one school year. It replaces the schedule of that grade. 404 for a year that the service does not know |
| POST   | `/commands/receive-payment`  | `ReceivePaymentCommandPayloadV1`: cash, at least 1 rupee. 201 with the receipt number |
| POST   | `/commands/reverse-charge`   | `ReverseChargeCommandPayloadV1`: the full charge, with a reason. 409 when it is already reversed |
| POST   | `/commands/reverse-payment`  | `ReversePaymentCommandPayloadV1`: the full payment, with a reason. The receipt number stays |
| GET    | `/fee-schedules`             | All schedules |
| GET    | `/accounts`                  | All accounts with `charged`, `paid` and `balance` (a negative balance is a credit) |
| GET    | `/accounts/:accountId`       | One account with its `charges` and `payments`, each with its `reversal` |

All money is `{ "amount": <paisa>, "currency": "PKR" }`. A command with the
same `commandId` again gives the first answer (200), not a second entry.

## How the fees come

```
ADMISSIONS stream            fees service                     FEES stream
AcademicYearOpened/Updated -> years
ClassOpened ----------------> classes
ClassClosed ----------------> remove the class
StudentEnrolled ------------> open account --> StudentAccountOpened -> fees.>
                              charge what is due --> FeeCharged
StudentClassChanged --------> new grade for the next charges
StudentLeft ----------------> no monthly fee after the leaving month
                              reverse the later monthly charges --> ChargeReversed
StudentLeavingCancelled ----> charge what is due again --> FeeCharged
```

- The stream on dev also has the old names from before v0.6.0
  (ClassSectionCreated/Updated, StudentSectionChanged, with `classSectionId`).
  The saga reads them as the new events, so a new consumer can read the
  stream from the start. Remove this at the prod go-live, with a new stream.

- The saga reads `ADMISSIONS` with the durable consumer `fees`, one message
  at a time, in stream order. Each event id goes into `handled` in the same
  transaction as its changes, so an event that comes twice changes nothing.
  The ack comes after the commit. On an error the message comes again in 5 s.
- What is due for an account:
  - the admission and annual fee of the schedule, once. If the grade has no
    schedule when the account opens, they come when the schedule is set.
  - the monthly fee for each month from the month the account opened (or the
    start of the year) to the current month in Pakistan (or the end of the
    year, or the month the student left). Due on the 10th, or on the day the
    account opened if that is later.
- When a student leaves, the leaving month stays charged. A monthly charge
  for a later month is reversed with the reason "The student left the school
  on <date>." and `reversed_for_leaving`. When the leaving is cancelled, these
  months are charged again, because the unique rule on account and month does
  not count them. A charge that the admin reversed stays reversed.
- The service charges what is due when an account opens, when a schedule is
  set, at start, and every hour. A charge exists at most once (unique rules
  on account and month, and on account and kind), so a second run adds
  nothing.
- A new schedule does not change charges that exist.

## Receipts

`R-<school year>-<6 digits>`, for example `R-2026-27-000001`: one counter for
each school year in `receipt_counters`, taken in the same transaction as the
payment, so a failed payment does not use a number. Two payments at the same
time lock the account one after the other and get the next two numbers.

## Events to NATS

The `events` table is the outbox, as in the-vps-admissions: every second the
relay publishes new rows to the stream `FEES` (`fees.>`), with
`Nats-Msg-Id` = `eventId`. The aggregate of a ledger entry is the account.

Settings: `DATABASE_URL`, `NATS_URL`, `NATS_USER` (`fees`), `NATS_PASSWORD`.

## Work on it

    npm ci
    npm run check
    TEST_DATABASE_URL=postgres://... TEST_NATS_URL=nats://localhost:4222 npm test

The tests use their own schema in the database and drop it at the end, and a
temporary stream in NATS.
