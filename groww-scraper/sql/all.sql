CREATE DATABASE ed;

CREATE TYPE asset_holding_type AS ENUM ('index', 'stocks', 'debt', 'stocks-elss');
ALTER TYPE asset_holding_type ADD VALUE 'stocks-mf';

create type investment_log_txn_type as enum ('REDEEM', 'PURCHASE');


-- manual entry table
drop table asset_holding_type_mapping;
CREATE TABLE asset_holding_type_mapping(
    schema_code varchar(20) PRIMARY KEY ,
    name varchar(50) not null,
    type asset_holding_type not null,
    expense_ratio numeric(10,4) not null,
    nav numeric(10,4) not null,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now()
);

DROP TABLE investments_log;
CREATE TABLE investments_log(
    id SERIAL,
    schema_code varchar(20) references asset_holding_type_mapping(schema_code),
    txn_price numeric(10,4) not null,
    txn_date timestamp without time zone not null,
    txn_type investment_log_txn_type not null,
    qty numeric(10,3) not null,
    total_amount numeric(10,2) not null,
    txn_id varchar(20) PRIMARY KEY,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now()
);

select * from asset_holding_type_mapping;
select * from investments_log;

-- investment split
WITH credit as (
    select schema_code, sum(total_amount) as credit
    from investments_log i_log
    WHERE i_log.txn_type = 'PURCHASE'
    group by schema_code
), debit as (
    SELECT schema_code, sum(total_amount) as debit
    FROM investments_log i_log
    WHERE i_log.txn_type = 'REDEEM'
    GROUP BY schema_code
)
SELECT type, sum(c.credit - coalesce(d.debit, 0)) from credit c
    left join debit d on c.schema_code = d.schema_code
left join asset_holding_type_mapping ass on c.schema_code = ass.schema_code
group by  type;

-- total networth on groww
WITH credit as (
    select schema_code, sum(total_amount) as credit
    from investments_log i_log
    WHERE i_log.txn_type = 'PURCHASE'
    group by schema_code
), debit as (
    SELECT schema_code, sum(total_amount) as debit
    FROM investments_log i_log
    WHERE i_log.txn_type = 'REDEEM'
    GROUP BY schema_code
)
SELECT sum(c.credit - coalesce(d.debit, 0)) as "invested-value" from credit c left join debit d on c.schema_code = d.schema_code;

-- current value
WITH credit as (
    select schema_code, sum(qty) as credit
    from investments_log i_log
    WHERE i_log.txn_type = 'PURCHASE'
    group by schema_code
), debit as (
    SELECT schema_code, sum(qty) as debit
    FROM investments_log i_log
    WHERE i_log.txn_type = 'REDEEM'
    GROUP BY schema_code
), qty_value as (
    SELECT c.schema_code, c.credit - coalesce(d.debit, 0) as qty_owned, ass.nav, ass.nav * (c.credit - coalesce(d.debit, 0)) as current_value  from credit c
    left join debit d on c.schema_code = d.schema_code
    left join asset_holding_type_mapping ass on c.schema_code = ass.schema_code
)
SELECT sum(current_value) as "current-value"
FROM qty_value;