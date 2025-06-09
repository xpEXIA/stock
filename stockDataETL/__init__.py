import tushare as ts
from stockDataETL.settings import tushare_token

ts.set_token(tushare_token)
ts_api = ts.pro_api()

