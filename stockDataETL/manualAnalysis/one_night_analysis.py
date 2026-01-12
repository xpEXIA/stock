from datetime import datetime
import sys
import pandas as pd
import requests

from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.manualAnalysis.database_conn import conn, engine
from stockDataETL.dataExtract.GetMyData import GetMyData
from pandas import DataFrame
from sqlalchemy import text

# 对近两周涨幅比较好的票进行统计分析
org_data = pd.read_sql(
    text("""
        select
            a.ts_code,
            a.pretrade_date,
            a.trade_date,
            b.name,
            b.pre_close,
            b.open,
            b.high,
            b.low,
            b.close,
            b.pct_chg,
            b.amout
        from (
            select
                ts_code,
                ods_trade_cal.pretrade_date pretrade_date,
                DATE_FORMAT(ods_trade_cal.cal_date,'%Y%m%d') as trade_date 
            from dm_daily_one_night_stock left join ods_trade_cal
                on DATE_FORMAT(update_time,'%Y%m%d') = ods_trade_cal.pretrade_date
            where ods_trade_cal.is_open = 1
              and dm_daily_one_night_stock.update_time < '2025-12-31'
        ) a left join (
            select
                ts_code,
                trade_date,
                name,
                pre_close,
                open,
                high,
                low,
                close,
                pct_chg,
                amout
            from dw_daily_trends
        ) b on a.ts_code = b.ts_code and a.trade_date = b.trade_date
    """),
    con=engine
)

index_daily = pd.read_sql(
    text("""
        select
            trade_date,
            open,
            high,
            low,
            close,
            pct_chg,
            amount
        from ods_index_daily
        where trade_date > '2025-06-30' and trade_date < '2025-12-31'
            and ts_code = '000001.SH'
    """),
    con=engine
)
index_daily['trade_date'] = index_daily.trade_date.apply(lambda x: x.strftime("%Y%m%d"))

up_down_num = pd.read_sql(
    text("""
        select
            trade_date,
            sum(
                (case 
                    when pct_chg > 0 then 1
                    else 0
                end)
            ) up_num,
            sum(
                (case 
                    when pct_chg = 0 then 1
                    else 0
                end)
            ) equal_num,
            sum(
                (case 
                    when pct_chg < 0 then 1
                    else 0
                end)
            ) down_num
        from dw_daily_trends
        where trade_date > '2025-06-30'
        group by trade_date
    """),
    con=engine
)
up_down_num['trade_date'] = up_down_num.trade_date.apply(lambda x: x.strftime("%Y%m%d"))

org_data['open_pct_chg'] = round((org_data.open - org_data.pre_close) / org_data.pre_close,4) * 100
org_data['high_pct_chg'] = round((org_data.high - org_data.pre_close) / org_data.pre_close,4) * 100
org_data['low_pct_chg'] = round((org_data.low - org_data.pre_close) / org_data.pre_close,4) * 100

open_mean = org_data.groupby("trade_date")["open_pct_chg"].mean().rename("open_mean").reset_index()
high_mean = org_data.groupby("trade_date")["high_pct_chg"].mean().rename("high_mean").reset_index()
low_mean = org_data.groupby("trade_date")["low_pct_chg"].mean().rename("low_mean").reset_index()

open_median = org_data.groupby("trade_date")["open_pct_chg"].median().rename("open_median").reset_index()
high_median = org_data.groupby("trade_date")["high_pct_chg"].median().rename("high_median").reset_index()
low_median = org_data.groupby("trade_date")["low_pct_chg"].median().rename("low_median").reset_index()

all_num = org_data.groupby("trade_date")["ts_code"].count().rename("all_num").reset_index()
open_up_num = org_data[org_data.open - org_data.pre_close > 0].groupby("trade_date")["ts_code"].count().rename("open_up_num").reset_index()
close_up_num = org_data[org_data.close - org_data.pre_close > 0].groupby("trade_date")["ts_code"].count().rename("close_up_num").reset_index()
high_up_num = org_data[org_data.high - org_data.pre_close > 0].groupby("trade_date")["ts_code"].count().rename("high_up_num").reset_index()

static2 = (
    open_mean.merge(high_mean, how="left", on="trade_date")
    .merge(low_mean, how="left",on="trade_date")
    .merge(open_median, how="left", on="trade_date")
    .merge(high_median, how="left", on="trade_date")
    .merge(low_median, how="left", on="trade_date")
    .merge(all_num, how="left", on="trade_date")
    .merge(open_up_num, how="left", on="trade_date")
    .merge(close_up_num, how="left", on="trade_date")
    .merge(high_up_num, how="left", on="trade_date")
    .merge(up_down_num, how="left", on="trade_date")
    .merge(index_daily, how="left", on="trade_date")
)

static2["open_up_ratio"] = round(static2.open_up_num / static2.all_num, 4) * 100
static2["close_up_ratio"] = round(static2.close_up_num / static2.all_num, 4) * 100
static2["high_up_ratio"] = round(static2.high_up_num / static2.all_num, 4) * 100

high_data = org_data[org_data.high - org_data.pre_close > 0]
high_data['high_pct'] = round((high_data.high - high_data.pre_close) / high_data.pre_close,4) * 100
high_pct_median = high_data.groupby("trade_date")["high_pct"].median().rename("high_pct_median").reset_index()

static2["low_median"].mean()
org_data[org_data.high_pct_chg > 0].low_pct_chg.mean()
org_data[org_data.high_pct_chg > 0].high_pct_chg.mean()
len(org_data[(org_data.high_pct_chg > 0) & (org_data.low_pct_chg > -1.5)].low_pct_chg)
len(org_data[(org_data.high_pct_chg > 0)].low_pct_chg)

