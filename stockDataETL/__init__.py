import tushare as ts
from stockDataETL.settings import tushare_token

ts.set_token(tushare_token)
ts_api = ts.pro_api()

import logging
logging = logging.basicConfig(
    filemode='w',
    filename='stock.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

