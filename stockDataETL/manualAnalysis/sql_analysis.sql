select
    *
from stock.dw_daily_trends
where trade_date = '2026-4-24' and market != '北交所'
order by net_mf_amount,net_d5_amount,ctu_up_days desc

select * from dm_daily_vol_unusual where trade_date = '2026-4-27'




