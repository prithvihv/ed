CREATE DATABASE ed;

CREATE TYPE asset_holding_type AS ENUM ('index', 'stocks', 'debt', 'stocks-elss');
ALTER TYPE asset_holding_type ADD VALUE 'stocks-mf';


-- manual entry table
drop table asset_holding_type_mapping;
CREATE TABLE asset_holding_type_mapping(
    schema_code varchar(20) PRIMARY KEY ,
    name varchar(50) not null,
    type asset_holding_type not null,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now()
);
select * from asset_holding_type_mapping;

DROP TABLE investments;
CREATE TABLE investments(
    id SERIAL,
    schema_code varchar(20) references asset_holding_type_mapping(schema_code),
    buy_price numeric(10,4) not null,
    buy_date timestamp without time zone not null,
    qty numeric(10,3) not null,
    total_amount numeric(10,2) not null,
    created_at timestamp without time zone default now(),
    updated_at timestamp without time zone default now()
);