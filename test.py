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
from stockDataETL.tasks.dm_daily_replay_daily_task import dm_daily_replay_daily_task
from stockDataETL.tasks.dm_stock_performance_daily_task import dm_stock_performance_daily_task
from stockDataETL.tasks.dm_up_limit_statistics_daily_task import dm_up_limit_statistics_daily_task


def test(request, date="fuck django"):
    dm_stock_performance_daily_task("20250819")
    return JsonResponse(
        {
            "status": "success",
            "message": date
        }
    )



