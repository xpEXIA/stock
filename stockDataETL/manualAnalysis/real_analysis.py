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
            market,
            ROUND(avg(circ_mv),2) avg_circ_mv,
            ROUND(avg(vol),2) avg_vol,
            ROUND(sum(net_mf_amount),2) net_5d_amount
        from dw_daily_trends
        where trade_date >= '2025-12-22' and trade_date <= '2025-12-26'
        group by ts_code,name,market
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
cal_data['vol_ratio'] = round((cal_data.vol / cal_time_num) * 240 / cal_data.avg_vol,2)
result = cal_data[
    (cal_data.pct_chg >= 3) & (cal_data.pct_chg <= 6) &
    (cal_data.avg_circ_mv >= 500000) & (cal_data.avg_circ_mv <= 2200000) &
    (cal_data.turnover_rate_f >= 5) & (cal_data.turnover_rate_f <= 15) &
#    (cal_data.net_5_amount > 0) &
    (cal_data.vol_ratio > 1) &
    ~cal_data.name.str.contains('ST',na=False)
]

result = result[[
    'update',
    'ts_code',
    'name',
    'market',
    'realtime',
    'open',
    'high',
    'low',
    'pre_close',
    'amout',
    'vol',
    'pct_chg',
    'pe',
    'turnover_rate_f',
    'vol_ratio',
    'avg_circ_mv',
    'avg_vol',
    'net_5_amount'
]].rename(columns={
    'update':'update_time',
})

result.to_sql('dm_daily_one_night_stock',con=engine,if_exists='append',index=False)


# cal_data = pd.read_sql(
#     text("""
#         select
#             trade_date,
#             ts_code,
#             turnover_rate_f,
#             pct_chg,
#             volume_ratio
#         from dw_daily_trends
#         where trade_date = '2025-11-14'
#     """),
#     con=engine
# )

# cal_data = pd.read_sql(
#     text("""
#         select
#             ts_code,
#             open,
#             close,
#             high,
#             pct_chg,
#             pre_close
#         from dw_daily_trends
#         where trade_date = '2025-11-17'
#     """),
#     con=engine
# )
#
# result = result.merge(cal_data, how='left', on=['ts_code'])
# result['isup'] = result['high'] - result['pre_close']
# result['openup'] = result['open'] - result['pre_close']
# print(len(result))
# print(len( result[result['openup'] > 0]))
# print(len( result[result['isup'] > 0]))
# print(len( result[result['pct_chg_y'] > 0]))
