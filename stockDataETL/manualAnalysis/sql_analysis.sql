select
    a.ts_code,
    a.name,
    `up_1.5_amount`,
    `up_0_amount`
from (
    select
        ts_code,
        name,
        count(id) as `up_1.5_amount`
    from stock.dw_daily_trends
    where trade_date >= '2025-04-08'
    and circ_mv <= 1500000
    and pct_chg > 1.5
    group by ts_code,name
) a left join (
    select
        ts_code,
        name,
        count(id) as `up_0_amount`
    from stock.dw_daily_trends
    where trade_date >= '2025-04-08'
    and circ_mv <= 1500000
    and pct_chg > 0
    group by ts_code,name
) b on a.ts_code = b.ts_code and a.name = b.name
order by `up_1.5_amount` desc;


select
    AVG(`up_1.5_amount`)
from (
    select
        ts_code,
        name,
        count(id) as `up_1.5_amount`
    from stock.dw_daily_trends
    where trade_date >= '2025-04-08'
    and circ_mv <= 1500000
    and pct_chg > 0
    group by ts_code,name
) a;



