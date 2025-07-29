from datetime import datetime

import pandas as pd
from django.http import JsonResponse
from stockDataETL import engine
from stockDataETL.dataExtract.GetTSData import GetTSData

from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily


def test(request):

    result = dm_stock_performance_daily(trade_date="2025-07-17")
    return JsonResponse(
        {
            "status": "success",
            "message": result
        }
    )


get_ts_data = GetTSData()
stock_list = get_ts_data.getStockBasic()
daily = get_ts_data.getDaily(start_date="20250709", end_date="20250709")
daily2 = get_ts_data.getDaily(start_date="20250710", end_date="20250710")
daily3 = get_ts_data.getDaily(start_date="20250708", end_date="20250708")
a = pd.merge(daily, daily2, on=['ts_code'], how="left")
b = a[(a["change_x"] > 0) & (a["change_y"] > 0)]
c = pd.merge(b, daily3, on=['ts_code'], how="left")
c = c[c['change'] > 0]
d = a[(a["change_x"] < 0) & (a["change_y"] > 0)]
stock_list.to_sql("stock_basic", con=engine, index=False, if_exists="replace")
stock_list=pd.DataFrame()

today = datetime.today()
a = today.strftime("%Y%m%d")
b = datetime.strptime(a, "%Y%m%d")

index_basic = get_ts_data.getIndexBasic(ts_code="000001.SH",market="SZSE")
