import pandas as pd

from stockDataETL import pymysql_connection
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL import ts_api
import tushare as ts




get_ts_data = GetTSData()
stock_list = get_ts_data.getStockBasic()
daily = get_ts_data.getDaily(start_date="20250709", end_date="20250709")
daily2 = get_ts_data.getDaily(start_date="20250710", end_date="20250710")
daily3 = get_ts_data.getDaily(start_date="20250708", end_date="20250708")
a = pd.merge(daily, daily2, on=['ts_code'], how="left")
b = a[(a["change_x"] > 0) & (a["change_y"] > 0)]
c = pd.merge(b, daily3, on=['ts_code'], how="left")
c = c[c['change'] > 0]
d = a[(a["change_x"] < 0) & (a["change_y"] > 0)]
stock_list.to_sql("stock_basic", con=pymysql_connection, index=False, if_exists="replace")




