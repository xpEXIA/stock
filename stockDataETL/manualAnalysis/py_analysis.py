from datetime import datetime
import sys
from sqlalchemy import create_engine
from stockDataETL.settings import DATABASES
import pandas as pd
from sqlalchemy import text

engine = create_engine(
    "mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8".format(
        user=DATABASES['default']['USER'],
        password=DATABASES['default']['PASSWORD'],
        host=DATABASES['default']['HOST'],
        port=DATABASES['default']['PORT'],
        db=DATABASES['default']['NAME']
    )
)

conn = engine.connect()


data = pd.read_sql("""select 
                      * 
                  from dw_daily_trends
                  where trade_date = '2025-08-04' or trade_date = '2025-08-07'
               """,engine)
data['trade_date'] = data['trade_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
now_data = data[data['trade_date'] == '2025-08-07']
pre_data = data[data['trade_date'] == '2025-08-04']
cal_data = pd.merge(now_data,pre_data[['ts_code','close','net_d5_amount']],
                    on='ts_code',how='left')
cal_data['up_pct'] = round((cal_data['close_x'] - cal_data['close_y']) / cal_data['close_y'],4) * 100
lg_circ_mv = cal_data[cal_data['circ_mv'] > 5000000]
up_10_data = cal_data[cal_data['up_pct'] > 10]
len(lg_circ_mv[lg_circ_mv['up_pct'] > 0])



