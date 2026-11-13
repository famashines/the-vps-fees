create table expenses (
  expense_id uuid primary key,
  spent_on date not null,
  category text not null,
  amount bigint not null check (amount >= 100),
  description text not null,
  paid_to text,
  recorded_at timestamptz not null,
  recorded_by uuid not null,
  reversed_at timestamptz,
  reversal_reason text,
  reversed_by uuid
);
create index expenses_spent_on on expenses (spent_on);
