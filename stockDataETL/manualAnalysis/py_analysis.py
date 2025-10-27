from datetime import datetime
import sys
import tushare as ts
from sqlalchemy import create_engine
from stockDataETL.settings import DATABASES
import pandas as pd
from pandas import DataFrame
from sqlalchemy import text

ts.set_token('5ea96f455496951c2719b8b3a8daf621b9c8c2ea7ed9d30041a4c867')
ts_api = ts.pro_api()
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
                  where trade_date >= '2025-09-15' and trade_date <= '2025-10-21'
               """,engine)
data['trade_date'] = data['trade_date'].apply(lambda x:x.strftime("%Y%m%d"))


now_data = data[data['trade_date'] == '2025-08-07']
pre_data = data[data['trade_date'] == '2025-06-23']
cal_data = pd.merge(now_data,pre_data[['ts_code','close','net_d5_amount']],
                    on='ts_code',how='left')
cal_data['up_pct'] = round((cal_data['close_x'] - cal_data['close_y']) / cal_data['close_y'],4) * 100
cal_data.sort_values(by='up_pct',ascending=False,inplace=True)
cal_data['up_pct'].median()
len(cal_data[cal_data['up_pct'] > 10])

pre_data = data[data['trade_date'] <= '20250811'].groupby('ts_code').agg({'vol':'mean','close':'median'}).reset_index()
vol_data = (data[data['trade_date'] == '20250811'][['ts_code','name','trade_date','close','pct_chg','market','vol','turnover_rate_f']]
            .merge(pre_data,on='ts_code',how='left'))
vol_data['vol_pct'] = round(vol_data['vol_x'] / vol_data['vol_y'],2)
vol_data['close_pct'] = round((vol_data['close_x'] - vol_data['close_y']) / vol_data['close_y'],4) * 100
vol_data.sort_values(by=['close_pct','vol_pct'],ascending=False,inplace=True)
cal_data = vol_data[(vol_data['vol_pct'] > 2.5) & (vol_data['market'] != '科创板')]

now_data = data[(data['trade_date'] <= '20250822') & (data['trade_date'] >= '20250812')].groupby(['ts_code','name'])['close'].max().reset_index()
now_data = now_data.merge(data[(data['trade_date'] <= '20250822') & (data['trade_date'] >= '20250812')][['ts_code','name','trade_date','close']],
                          on=['ts_code','name','close'],how='left')
now_data.drop_duplicates(subset=['ts_code','name'],keep='first',inplace=True)

up_data = cal_data.merge(now_data,on=['ts_code','name'],how='left')
up_data['close_max_pct'] = round((up_data['close'] - up_data['close_x']) / up_data['close_x'],4) * 100
up_data['days'] = (up_data['trade_date_y'].astype('datetime64[ns]') - up_data['trade_date_x'].astype('datetime64[ns]')).dt.days

up_data_turnover = up_data[up_data['turnover_rate_f'] >= 20]

all_data = pd.merge(data[data['trade_date'] == '20250811'][['ts_code','name','trade_date','close']],
                    now_data,on=['ts_code','name'],how='left')
all_data['close_max_pct'] = round((all_data['close_y'] - all_data['close_x']) / all_data['close_x'],4) * 100


data = pd.read_sql("""select 
            trade_date,
            ts_code,
            close,
            open,
            high,
            pre_close,
            open_pct_chg,
            up_limit
        from dw_daily_trends
        where trade_date >= '2025-08-26' and trade_date <= '2025-08-27'
               """,engine)
data['trade_date'] = data['trade_date'].apply(lambda x:x.strftime("%Y%m%d"))
a = {}
a['up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_date'] == '20250827')]['close'].count()
