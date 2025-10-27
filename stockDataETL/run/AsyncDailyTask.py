from datetime import datetime
from functools import partial

from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger
from django.http import request, JsonResponse
from asyncio import gather, get_event_loop
from stockDataETL.tasks.dm_daily_vol_unusual_daily_task import dm_daily_vol_unusual_daily_task
from stockDataETL.tasks.ods_stock_basic_task import ods_stock_basic_task
from stockDataETL.tasks.ods_daily_task import ods_daily_task
from stockDataETL.tasks.ods_daily_basic_task import ods_daily_basic_task
from stockDataETL.tasks.ods_stk_limit_task import ods_stk_limit_task
from stockDataETL.tasks.ods_moneyflow_task import ods_moneyflow_task
from stockDataETL.tasks.ods_index_daily_task import ods_index_daily_task
from stockDataETL.tasks.dw_daily_trends_daily_task import dw_daily_trends_daily_task
from stockDataETL.tasks.dm_stock_performance_daily_task import dm_stock_performance_daily_task
from stockDataETL.tasks.dm_daily_replay_daily_task import dm_daily_replay_daily_task
from stockDataETL.tasks.dm_up_limit_statistics_daily_task import dm_up_limit_statistics_daily_task

async def asyncDailyTask(request):
    """异步处理每日任务"""
    get_TS_data = GetTSData()
    # data_load = DataLoad()
    failure_list = []
    loop = get_event_loop()
    logger.info("开始获取ods数据")
    date = request.GET.get('date')

    if date:
        today = date
    else:
        today = datetime.today().strftime("%Y%m%d")
    
    # 首先执行getTradeCal
    get_trade_cal = get_TS_data.getTradeCal(start_date=today, end_date=today)
    cal_date = get_trade_cal['cal_date'].values[0]
    
    if get_trade_cal['is_open'].values[0] != 1:
        # data_load.close()
        return JsonResponse(
            {
                "status": "error",
                "message": "非交易日"
            }
        )
    else:        
        # 异步执行所有通过get_TS_data请求接口获取数据的代码
        ods_tasks = [
            loop.run_in_executor(None, ods_stock_basic_task),
            loop.run_in_executor(None, partial(ods_daily_task, trade_date=cal_date)),
            loop.run_in_executor(None, partial(ods_daily_basic_task, trade_date=cal_date)),
            loop.run_in_executor(None, partial(ods_stk_limit_task, trade_date=cal_date)),
            loop.run_in_executor(None, partial(ods_moneyflow_task, trade_date=cal_date)),
            loop.run_in_executor(None, partial(ods_index_daily_task, trade_date=cal_date)),
        ]

        # 等待所有异步任务完成
        results = await gather(*ods_tasks, return_exceptions=True)

        # 处理结果
        for result in results:
            if result is not None:
                failure_list.append(result)
        
        trade_date = datetime.strptime(get_trade_cal['cal_date'].values[0], "%Y%m%d").strftime("%Y-%m-%d")
        
        # 同步执行dw_daily_trends操作
        dw_result = dw_daily_trends_daily_task(trade_date=trade_date)
        if dw_result is not None:
            failure_list.append(dw_result)

        
        # 异步处理dm层数据
        dm_tasks = [
            loop.run_in_executor(None, partial(dm_daily_replay_daily_task, trade_date=trade_date)),
            loop.run_in_executor(None, partial(dm_stock_performance_daily_task, trade_date=trade_date)),
            loop.run_in_executor(None, partial(dm_up_limit_statistics_daily_task, trade_date=trade_date)),
            loop.run_in_executor(None, partial(dm_daily_vol_unusual_daily_task, trade_date=trade_date)),
        ]
        
        # 等待剩余的异步任务完成
        dm_results = await gather(*dm_tasks, return_exceptions=True)
        
        # 处理剩余操作的结果
        for dm_result in dm_results:
            if dm_result is not None:
                failure_list.append(dm_result)
        
        # data_load.close()
        if failure_list == []:
            logger.info("每日数据获取成功")
            return JsonResponse(
                {
                    "status": "success",
                    "message": "每日数据获取成功"
                }
            )
        logger.error(f"每日数据获取失败, 失败表: {failure_list}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"每日数据获取失败, 失败表: {failure_list}"
            }
        )