from datetime import datetime

import pandas as pd
from django.http import JsonResponse
from pandas import DataFrame

from stockDataETL import engine, logger
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad

from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataTransform.dm_daily_vol_unusual import dm_daily_vol_unusual
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataTransform.dm_up_limit_statistics_daily import dm_up_limit_statistics_daily
from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily
from stockDataETL.tasks.dm_daily_replay_daily_task import dm_daily_replay_daily_task
from stockDataETL.tasks.dm_daily_vol_unusual_daily_task import dm_daily_vol_unusual_daily_task
from stockDataETL.tasks.dm_stock_performance_daily_task import dm_stock_performance_daily_task
from stockDataETL.tasks.dm_up_limit_statistics_daily_task import dm_up_limit_statistics_daily_task


def test(request, date="fuck django"):
    # dm_stock_performance_daily_task("2025-09-10")

    # data_load = DataLoad()
    # cal_data = data_load.search(
    #     """select cal_date, is_open
    #        from ods_trade_cal
    #        where cal_date >= :end_date
    #          and is_open = 1
    #     """,
    #     {
    #         "end_date": '20250901'
    #     }
    # )
    # cal_data = DataFrame(cal_data, columns=["cal_date", "is_open"])
    # cal_data['cal_date'] = cal_data['cal_date'].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d"))
    # for i in cal_data['cal_date'].to_list():
    #     dm_daily_vol_unusual_daily_task(i)

    dm_daily_vol_unusual_daily_task('2025-10-13')
    dm_daily_vol_unusual_daily_task('2025-10-20')
    # dm_up_limit_statistics_daily_task("2025-08-27")
    return JsonResponse(
        {
            "status": "success",
            "message": date
        }
    )



