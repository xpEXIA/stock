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
table_list = ['dw_daily_trends',
                  "dm_daily_replay", "dm_stock_performance", "dm_up_limit_statistics"]
for table_name in table_list:
    conn.execute(text(f"TRUNCATE TABLE {table_name};"))

data = pd.read_sql("""select 
                      * 
                  from dw_daily_trends
                  where trade_date >= '2025-04-07'
               """,engine)
data['trade_date'] = data['trade_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
memory_size = sys.getsizeof(data)
print(f"数据占用内存: {memory_size / (1024 * 1024):.2f} MB")
total_mv = (data[data['pct_chg'] > 1.5]
          .groupby(['ts_code','name'])
          .agg({'id': 'count'}).reset_index()).sort_values('id', ascending=False)
low_mv = (data[(data['circ_mv'] < 1000000.00)
               & (data['circ_mv'] >= 500000.00)
               & (data['pct_chg'] > 1.5)]
          .groupby(['ts_code','name'])
          .agg({'id': 'count'}).reset_index()).sort_values('id', ascending=False)
high_mv = (data[(data['circ_mv'] >= 1500000.00) & (data['pct_chg'] > 1.5)]
          .groupby(['ts_code','name'])
          .agg({'id': 'count'}).reset_index()).sort_values('id', ascending=False)
small_mv = (data[(data['circ_mv'] <= 500000.00)
                  & (data['circ_mv'] >= 200000.00)
                 & (data['pct_chg'] > 1.5)]
          .groupby(['ts_code','name'])
          .agg({'id': 'count'}).reset_index()).sort_values('id', ascending=False)
now_data = (data[data['trade_date'] == '2025-04-08'][['ts_code','name','industry','close','circ_mv','pe']]
            .merge(data[data['trade_date'] == '2025-08-01'][['ts_code','close']],how='left',on='ts_code'))
now_data['pct_chg'] = round(((now_data['close_y'] - now_data['close_x']) / now_data['close_x']) * 100)
now_data.sort_values('pct_chg', ascending=False, inplace=True)
now_data_50 = now_data[(now_data['pct_chg'] > 50) & (now_data['circ_mv'] < 1600000.00)]
len(now_data[now_data['circ_mv'] < 250000])
len(now_data_50[now_data_50['circ_mv'] < 250000])
