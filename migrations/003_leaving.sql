alter table accounts add column left_on date;
alter table charges add column reversed_for_leaving boolean not null default false;
drop index charges_monthly_once;
create unique index charges_monthly_once on charges (account_id, month) where kind = 'monthly' and not reversed_for_leaving;
