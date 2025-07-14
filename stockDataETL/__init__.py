import tushare as ts
from stockDataETL.settings import tushare_token, DATABASES
import logging
import pymysql
from sqlalchemy import create_engine

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

# SQLAlchemy 数据库连接
engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8".format(
        user=DATABASES['default']['USER'],
        password=DATABASES['default']['PASSWORD'],
        host=DATABASES['default']['HOST'],
        port=DATABASES['default']['PORT'],
        db=DATABASES['default']['NAME']
    )
)

# pymysql 数据库连接
# pymysql_connection = pymysql.connect(
#     host=DATABASES['default']['HOST'],
#     port=DATABASES['default']['PORT'],
#     user=DATABASES['default']['USER'],
#     password=DATABASES['default']['PASSWORD'],
#     db=DATABASES['default']['NAME'],
#     charset="utf8"
# )

