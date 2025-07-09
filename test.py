
from stockDataETL.scripts.DataExtract import GetTSData
from stockDataETL.settings import tushare_token
import tushare as ts
from django.db import connection

ts.set_token(tushare_token)
ts_api = ts.pro_api()

get_ts_data = GetTSData()
daily = get_ts_data.getDaily(start_date="20250708", end_date="20250708")
stock_connect =  connection['stock'].cursor.db.connection
daily.to_sql('daily', connection, if_exists='append', index=False)






