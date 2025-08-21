set @trade_date = '2025-08-15';
select count(id) as ods_daily  from ods_daily where trade_date = @trade_date;
select count(id) as ods_daily_basic from ods_daily_basic where trade_date = @trade_date;
select count(id) as ods_index_daily from ods_index_daily where trade_date = @trade_date;
select count(id) as ods_moneyflow from ods_moneyflow where trade_date = @trade_date;
select count(id) as ods_stk_limit from ods_stk_limit where trade_date = @trade_date;
select count(*) as ods_stock_basic from ods_stock_basic;

select count(id) as dm_daily_replay from dm_daily_replay where trade_date = @trade_date;
select count(id) as dm_up_limit_statistics from dm_up_limit_statistics where trade_date = @trade_date;
select count(id) as dm_stock_performance from dm_stock_performance where trade_date = @trade_date;
select count(id) as dw_daily_trends from dw_daily_trends where trade_date = @trade_date;

