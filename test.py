from datetime import datetime

import pandas as pd
from django.http import JsonResponse
from stockDataETL import engine, logger
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad

from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataTransform.dm_up_limit_statistics_daily import dm_up_limit_statistics_daily
from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily


def test(request):

    data_load = DataLoad()
    data_load.truncate('dm_up_limit_statistics')

    trade_date_list = pd.read_sql("""select
                          cal_date
                      from ods_trade_cal
                      where ods_trade_cal.cal_date <= '20250807'
                      and is_open = 1
                   """, engine)
    trade_date_list = trade_date_list["cal_date"].tolist()
    trade_date_list = list(map(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d"), trade_date_list))
    trade_date_list.reverse()
    for trade_date in trade_date_list[1:]:
        dm_up_limit_statistics_daily(trade_date=trade_date, connect=data_load)
    data_load.close()
    return JsonResponse(
        {
            "status": "success",
            "message": 'fucking data'
        }
    )



