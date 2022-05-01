CREATE DATABASE ed;

CREATE TYPE asset_holding_type AS ENUM ('index', 'stocks', 'debt', 'stocks-elss');
ALTER TYPE asset_holding_type ADD VALUE 'stocks-mf';

create type investment_log_txn_type as enum ('REDEEM', 'PURCHASE');


-- manual entry table
drop table asset_holding_type_mapping;
drop table asset_tick_data;
DROP TABLE investments_log;

CREATE TABLE asset_holding_type_mapping(
    id SERIAL,
    schema_code varchar(20) PRIMARY KEY,
    name varchar(50) not null,
    type asset_holding_type not null,
    expense_ratio numeric(10,4) not null,
    nav numeric(10,4) not null,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now()
);

CREATE TABLE asset_tick_data(
    id SERIAL,
    schema_code varchar(20) references asset_holding_type_mapping(schema_code),
    tick_value numeric(10,4) not null,
    tick_date timestamp without time zone not null,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now(),

    UNIQUE (tick_date, tick_value, schema_code)
);

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

-- asset performance
SELECT
  tick_date AS "time",
  name AS metric,
  tick_value
FROM asset_tick_data
LEFT JOIN asset_holding_type_mapping ahtm on asset_tick_data.schema_code = ahtm.schema_code
WHERE
  asset_tick_data.schema_code = $asset_schema_name
ORDER BY 1,2


-- asset info
SELECT
  schema_code,
  name,
  type,
  nav,
  expense_ratio
FROM asset_holding_type_mapping
left join ;
WHERE
  schema_code = $asset_schema_name
ORDER BY 1,2

-- TODO: derive asset_current value, asset_invested, asset growth, asset_cagr
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

SELECT * from asset_holding_type_mapping;
SELECT * from asset_tick_data where tick_date = '2017-03-27'::timestamp + interval '6 days';

-- CAGR PRICES
SELECT tick_value from asset_tick_data
WHERE schema_code = '120503'
    and (tick_date = '2017-03-27'::timestamp
             OR tick_date = '2017-03-27'::timestamp + interval '1 year')
ORDER BY tick_date asc

-- unique mf names
SELECT distinct ahtm.name from investments_log
left join asset_holding_type_mapping ahtm on ahtm.schema_code = investments_log.schema_code