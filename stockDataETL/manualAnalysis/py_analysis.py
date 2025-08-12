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
                  where trade_date >= '2025-07-15' and trade_date <= '2025-08-12'
               """,engine)
data['trade_date'] = data['trade_date'].apply(lambda x:x.strftime("%Y-%m-%d"))
now_data = data[data['trade_date'] == '2025-08-07']
pre_data = data[data['trade_date'] == '2025-06-23']
cal_data = pd.merge(now_data,pre_data[['ts_code','close','net_d5_amount']],
                    on='ts_code',how='left')
cal_data['up_pct'] = round((cal_data['close_x'] - cal_data['close_y']) / cal_data['close_y'],4) * 100
cal_data.sort_values(by='up_pct',ascending=False,inplace=True)
cal_data['up_pct'].median()
len(cal_data[cal_data['up_pct'] > 10])
industry_data = cal_data[['industry','up_pct']].groupby('industry').agg({'up_pct':'median'}).sort_values(by='up_pct',ascending=False)

pre_data = data[data['trade_date'] != '2025-08-12'].groupby('ts_code').agg({'vol':'mean','close':'mean'}).reset_index()
vol_data = (data[data['trade_date'] == '2025-08-12'][['ts_code','name','trade_date','close','pct_chg','market','vol']]
            .merge(pre_data,on='ts_code',how='left'))
vol_data['vol_pct'] = round(vol_data['vol_x'] / vol_data['vol_y'],2)
vol_data['close_pct'] = round((vol_data['close_x'] - vol_data['close_y']) / vol_data['close_y'],4) * 100
vol_data.sort_values(by=['close_pct','vol_pct'],ascending=False,inplace=True)
asd = vol_data[(vol_data['vol_pct'] > 3) & (vol_data['market'].isin(['主板','创业板']))]
