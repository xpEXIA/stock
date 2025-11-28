from datetime import datetime,timedelta
import requests
from sqlalchemy import text
from stockDataETL.manualAnalysis.database_conn import engine
import pandas as pd
from stockDataETL.dataExtract.GetMyData import GetMyData

stock_list = pd.read_sql(
    text("""
        select
            ts_code
        from stock.ods_stock_basic
    """),
    con=engine
)

avg_vol_5 = pd.read_sql(
    text("""
        select
            ts_code,
            name,
            avg(circ_mv) avg_circ_mv,
            avg(vol) avg_vol,
            sum(net_mf_amount) net_5_amount
        from dw_daily_trends
        where trade_date >= '2025-11-24'
        group by ts_code,name
    """),
    con=engine
)

getMydata = GetMyData()
cal_data = getMydata.getRealTimeDaily(ts_code_list=stock_list.ts_code.tolist())
cal_data = cal_data.merge(avg_vol_5, how='left', on=['ts_code'])
cal_time = cal_data['update'].iloc[0]
cal_time = datetime.strptime(cal_time, "%Y-%m-%d %H:%M:%S")
if cal_time.hour < 13:
    cal_time_num = cal_time - datetime(cal_time.year,cal_time.month,cal_time.day,9,30,0)
    cal_time_num = cal_time_num.seconds / 60
else:
    cal_time_num = cal_time - datetime(cal_time.year,cal_time.month,cal_time.day,13,0,0)
    cal_time_num = cal_time_num.seconds / 60 + 120
cal_data['volume_ratio'] = (cal_data.vol / cal_time_num) * 240 / cal_data.avg_vol
result = cal_data[
    (cal_data.pct_chg >= 3) & (cal_data.pct_chg <= 5) &
    (cal_data.avg_circ_mv >= 500000) & (cal_data.avg_circ_mv <= 2200000) &
    (cal_data.turnover_ratio_f >= 5) & (cal_data.turnover_ratio_f <= 12) &
#    (cal_data.net_5_amount > 0) &
    (cal_data.volume_ratio > 1) &
    ~cal_data.name.str.contains('ST',na=False)
]

