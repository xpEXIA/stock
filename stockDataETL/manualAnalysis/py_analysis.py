from datetime import datetime
import sys
import pandas as pd
import requests

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
            a.circ_mv,
            a.pe,
            ROUND(((b.close - a.close) / a.close), 4) * 100 pct
        from (
            select
                ts_code,
                name,
                industry,
                market,
                circ_mv,
                pe,
                close
            from stock.dw_daily_trends
            where trade_date = '2025-10-20'
        ) a left join (
            select
                ts_code,
                close
            from stock.dw_daily_trends
            where trade_date = '2025-11-07'
        ) b on a.ts_code = b.ts_code
        where a.market in ('主板','创业板')
        order by pct desc
    """),
    con=engine
)

small_mv = org_data[org_data.circ_mv < 1500000]
small_mv_up = small_mv[small_mv.pct >= 0]
large_mv = org_data[org_data.circ_mv >= 1500000]
large_mv_up = large_mv[large_mv.pct >= 0]

len(small_mv_up) / len(small_mv)
len(large_mv_up) / len(large_mv)
small_mv_up.pct.median()
large_mv_up.pct.median()

getMyData = GetMyData()
a = getMyData.getRealTimeDaily(["000001", "000002", "000004"])