from datetime import datetime
import sys
import pandas as pd
import requests

from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.manualAnalysis.database_conn import conn, engine
from stockDataETL.dataExtract.GetMyData import GetMyData
from pandas import DataFrame
from sqlalchemy import text


# 对近两周涨幅比较好的票进行统计分析
org_data = pd.read_sql(
    text("""
        select
            a.ts_code,
            a.name,
            a.industry,
            a.market,
            a.close,
            ROUND((a.close - b.close) / b.close,4) * 100 up_pct_chg,
            pct_chg,
            amout,
            circ_mv,
            turnover_rate_f,
            pe,
            volume_ratio,
            net_d5_amount,
            net_mf_amount,
            ctu_up_days
        from (
            select
                ts_code,
                name,
                industry,
                market,
                close
            from dw_daily_trends
            where trade_date = '2026-01-09'
        ) a left join (
            select
                trade_date,
                ts_code,
                close,
                pct_chg,
                amout,
                circ_mv,
                turnover_rate_f,
                pe,
                volume_ratio,
                net_d5_amount,
                net_mf_amount,
                ctu_up_days
            from dw_daily_trends
            where trade_date = '2026-01-08'
        ) b on a.ts_code = b.ts_code
    """),
    con=engine
)

up_data = org_data[org_data["pct_chg"] >= 10]
len(up_data[up_data.up_pct_chg >= 0])
len(up_data)