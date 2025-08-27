SET @trade_date = '2025-08-22';
select * from ods_daily where trade_date = @trade_date;
select * from ods_daily_basic where trade_date = @trade_date;
select * from ods_index_daily where trade_date = @trade_date;
select * from ods_moneyflow where trade_date = @trade_date;
select * from ods_stk_limit where trade_date = @trade_date;
select * from ods_stock_basic;

select * from dm_daily_replay where trade_date = @trade_date;
select * from dm_up_limit_statistics where trade_date = @trade_date;
select * from dm_stock_performance where trade_date = @trade_date;
select * from dw_daily_trends where trade_date = @trade_date;

