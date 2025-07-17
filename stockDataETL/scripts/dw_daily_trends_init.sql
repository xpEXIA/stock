# SET @today = date_format(current_date(),'%Y-%m-%d');
SET @today = date_format(date_sub(current_date(),interval 1 day ),'%Y-%m-%d');
# SET @yesterday = date_format(date_sub(current_date(),interval 1 day ),'%Y-%m-%d');
SET @yesterday = date_format(date_sub(current_date(),interval 2 day ),'%Y-%m-%d');

Insert Into dw_daily_trends (ts_code, trade_date,open, high, low, close, pre_close, pct_chg,
                            open_pct_chg, high_pct_chg, low_pct_chg, vol, amout, name, industry,market,
                            turnover_rate_f, volume_ratio, pe, total_mv, circ_mv, up_limit, down_limit, pre_high,
                            buy_sm_amount, sell_sm_amount,buy_md_amount, sell_md_amount, buy_lg_amount,
                             sell_lg_amount, buy_elg_amount,sell_elg_amount, net_mf_amount, buy_elg_net_amount,
                             buy_elg_net_amount_rate, buy_lg_net_amount, buy_lg_net_amount_rate, buy_md_net_amount,
                             buy_md_net_amount_rate, buy_sm_net_amount, buy_sm_net_amount_rate,
                             net_d5_amount, up_days)
select
    ods_daily.ts_code as ts_code,
    ods_daily.trade_date as trade_date,
    ods_daily.open as open,
    ods_daily.high as high,
    ods_daily.low as low,
    ods_daily.close as close,
    ods_daily.pre_close as pre_close,
    ods_daily.pct_chg as pct_chg,
    (ods_daily.open - ods_daily.pre_close) / ods_daily.pre_close as open_pct_chg,
    (ods_daily.high - ods_daily.pre_close) / ods_daily.pre_close as high_pct_chg,
    (ods_daily.low - ods_daily.pre_close) / ods_daily.pre_close as low_pct_chg,
    ods_daily.vol as vol,
    ods_daily.amount as amount,
    ods_stock_basic.name as name,
    ods_stock_basic.industry as industry,
    ods_stock_basic.market as market,
    ods_daily_basic.turnover_rate_f as turnover_rate_f,
    ods_daily_basic.volume_ratio as volume_ratio,
    ods_daily_basic.pe as pe,
    ods_daily_basic.total_mv as total_mv,
    ods_daily_basic.circ_mv as cric_mv,
    ods_stk_limit.up_limit as up_limit,
    ods_stk_limit.down_limit as down_limit,
    pre_daily.high as pre_high,
    ods_moneyflow.buy_sm_amount as buy_sm_amount,
    ods_moneyflow.sell_sm_amount as sell_sm_amount,
    ods_moneyflow.buy_md_amount as buy_md_amount,
    ods_moneyflow.sell_md_amount as sell_md_amount,
    ods_moneyflow.buy_lg_amount as buy_lg_amount,
    ods_moneyflow.sell_lg_amount as sell_lg_amount,
    ods_moneyflow.buy_elg_amount as buy_elg_amount,
    ods_moneyflow.sell_elg_amount as sell_elg_amount,
    ods_moneyflow.net_mf_amount as net_mf_amount,
    ods_moneyflow.buy_elg_amount - ods_moneyflow.sell_elg_amount as buy_elg_net_amount,
    ROUND(((ods_moneyflow.buy_elg_amount - ods_moneyflow.sell_elg_amount) / net_mf_amount),2) as buy_elg_net_amount_rate,
    ods_moneyflow.buy_lg_amount - ods_moneyflow.sell_lg_amount as buy_lg_net_amount,
    ROUND(((ods_moneyflow.buy_lg_amount - ods_moneyflow.sell_lg_amount) / net_mf_amount),2) as buy_lg_net_amount_rate,
    ods_moneyflow.buy_md_amount - ods_moneyflow.sell_md_amount as buy_md_net_amount,
    ROUND(((ods_moneyflow.buy_md_amount - ods_moneyflow.sell_md_amount) / net_mf_amount),2) as buy_md_net_amount_rate,
    ods_moneyflow.buy_sm_amount - ods_moneyflow.sell_sm_amount as buy_sm_net_amount,
    ROUND(((ods_moneyflow.buy_sm_amount - ods_moneyflow.sell_sm_amount) / net_mf_amount),2) as buy_sm_net_amount_rate,
    net_d5.net_d5_amount as net_d5_amount,
    case
        when ods_daily.pct_chg > 0 then (
            case
                when pre_daily_trends.up_days is Null then 0
                else pre_daily_trends.up_days
            end
        ) + 1
        else 0
    end as up_days
from ods_daily left join ods_stock_basic
        on ods_daily.ts_code = ods_stock_basic.ts_code
    left join ods_daily_basic
        on ods_daily.ts_code = ods_daily_basic.ts_code and ods_daily.trade_date = ods_daily_basic.trade_date
    left join (
            select
                ods_daily.high as high,
                ods_daily.ts_code as ts_code
            from ods_daily
            where trade_date = @yesterday
    ) pre_daily
        on ods_daily.ts_code = pre_daily.ts_code
    left join ods_stk_limit
        on ods_daily.ts_code = ods_stk_limit.ts_code and ods_daily.trade_date = ods_stk_limit.trade_date
    left join ods_moneyflow
        on ods_daily.ts_code = ods_moneyflow.ts_code and ods_daily.trade_date = ods_moneyflow.trade_date
    left join (
            select
                ods_moneyflow.ts_code as ts_code,
                (sum(ods_moneyflow.buy_lg_amount + ods_moneyflow.buy_elg_amount)
            - sum(ods_moneyflow.sell_lg_amount + ods_moneyflow.sell_elg_amount)) as net_d5_amount
            from ods_moneyflow
            where trade_date <= @today and trade_date >= date_sub(@today, interval 5 day)
            group by ods_moneyflow.ts_code
    ) net_d5
        on ods_daily.ts_code = net_d5.ts_code
    left join (
            select
                dw_daily_trends.ts_code as ts_code,
                dw_daily_trends.up_days as up_days
            from dw_daily_trends
            where trade_date = @yesterday
    ) pre_daily_trends
        on ods_daily.ts_code = pre_daily_trends.ts_code
where ods_daily.trade_date = @today

