import tushare as ts
from stockDataETL.settings import tushare_token, DATABASES
import logging
import pymysql

# 日志配置
logging.basicConfig(
    filemode='w',
    filename='stock.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# tushare 配置
ts.set_token(tushare_token)
ts_api = ts.pro_api()

# pymysql 数据库连接
pymysql_connection = pymysql.connect(
    host=DATABASES['HOST'],
    port=DATABASES['PORT'],
    user=DATABASES['USER'],
    password=DATABASES['PASSWORD'],
    db=DATABASES['NAME'],
    charset=DATABASES['UTF8']
)

