create table handled (
  message_id uuid primary key,
  result jsonb not null,
  handled_at timestamptz not null default now()
);

create table events (
  event_id uuid primary key,
  event_type text not null,
  version int not null,
  aggregate_id uuid not null,
  aggregate_version int not null,
  occurred_at timestamptz not null,
  correlation_id uuid not null,
  causation_id uuid not null,
  issued_by uuid,
  payload jsonb not null,
  published_at timestamptz,
  unique (aggregate_id, aggregate_version)
);
create index events_unpublished on events (occurred_at, aggregate_version) where published_at is null;

create table years (
  academic_year_id uuid primary key,
  name text not null,
  starts_on date not null,
  ends_on date not null
);

create table sections (
  class_section_id uuid primary key,
  academic_year_id uuid not null references years,
  grade text not null
);

create table fee_schedules (
  fee_schedule_id uuid primary key,
  academic_year_id uuid not null,
  grade text not null,
  monthly_fee bigint not null check (monthly_fee >= 0),
  admission_fee bigint check (admission_fee >= 0),
  annual_fee bigint check (annual_fee >= 0),
  version int not null,
  constraint fee_schedules_year_fk foreign key (academic_year_id) references years,
  constraint fee_schedules_one_per_grade unique (academic_year_id, grade)
);

create table accounts (
  account_id uuid primary key,
  student_id uuid not null,
  enrollment_id uuid not null unique,
  academic_year_id uuid not null references years,
  class_section_id uuid not null,
  grade text not null,
  opened_on date not null,
  one_time_charged boolean not null default false,
  version int not null
);

create table charges (
  charge_id uuid primary key,
  account_id uuid not null references accounts,
  kind text not null check (kind in ('admission', 'annual', 'monthly')),
  month text check ((kind = 'monthly') = (month is not null)),
  amount bigint not null check (amount >= 0),
  due_on date not null,
  charged_at timestamptz not null,
  reversed_at timestamptz,
  reversal_reason text,
  reversed_by uuid
);
create unique index charges_monthly_once on charges (account_id, month) where kind = 'monthly';
create unique index charges_one_time_once on charges (account_id, kind) where kind <> 'monthly';

create table receipt_counters (
  academic_year_id uuid primary key,
  last int not null
);

create table payments (
  payment_id uuid primary key,
  account_id uuid not null references accounts,
  receipt_number text not null unique,
  amount bigint not null check (amount >= 100),
  method text not null check (method = 'cash'),
  received_at timestamptz not null,
  received_by uuid not null,
  reversed_at timestamptz,
  reversal_reason text,
  reversed_by uuid
);
