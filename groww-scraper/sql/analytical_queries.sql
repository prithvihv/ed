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