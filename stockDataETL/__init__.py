import os
import sys

import tushare as ts
from stockDataETL.settings import tushare_token, DATABASES
import logging
import pymysql
from sqlalchemy import create_engine

# 业务日志配置
logger = logging.getLogger(__name__)
# 自己配置会导致日志写两遍……
# logger.basicConfig(
#     # filemode='w',
#     # filename='stock.log',
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler(filename='stock.log',mode='a',encoding='utf-8'),
#         logging.StreamHandler()
#     ]
# )


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

