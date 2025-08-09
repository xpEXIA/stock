from datetime import datetime
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger
from django.http import request, JsonResponse
from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataTransform.dm_up_limit_statistics_daily import dm_up_limit_statistics_daily
from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily
import asyncio

async def dailyTask(request, date: str = None):
    """异步处理每日任务"""
    get_TS_data = GetTSData()
    data_load = DataLoad()
    failure_list = []
    logger.info("开始获取ods数据")

    if date:
        today = date
    else:
        today = datetime.today().strftime("%Y%m%d")
    
    # 首先执行getTradeCal
    get_trade_cal = get_TS_data.getTradeCal(start_date=today, end_date=today)
    
    if get_trade_cal['is_open'].values[0] != 1:
        data_load.close()
        return JsonResponse(
            {
                "status": "error",
                "message": "非交易日"
            }
        )
    else:
        logger.info("开始更新股票基础数据, 表ods_stock_basic")
        data_load.truncate("ods_stock_basic")
        
        # 异步执行所有通过get_TS_data请求接口获取数据的代码
        tasks = []
        
        # 添加获取股票基础数据的任务
        tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getStockBasic))
        
        # 添加获取日线行情数据的任务
        tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getDaily, get_trade_cal['cal_date'].values[0]))
        
        # 添加获取每日指标数据的任务
        tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getDailyBasic, get_trade_cal['cal_date'].values[0]))
        
        # 添加获取指数日线行情数据的任务
        index_code = ["000001.SH", "399107.SZ"]
        index_tasks = []
        for index_code_ in index_code:
            index_tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getIndexDaily, index_code_, get_trade_cal['cal_date'].values[0]))
        
        # 添加获取资金流向数据的任务
        tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getMoneyFlow, get_trade_cal['cal_date'].values[0]))
        
        # 添加获取股票涨跌幅数据的任务
        tasks.append(asyncio.get_event_loop().run_in_executor(None, get_TS_data.getStkLimit, get_trade_cal['cal_date'].values[0]))
        
        # 等待所有异步任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理结果
        get_stock_basic, get_daily, get_daily_basic, get_moneyflow, get_stk_limit = results[:5]
        index_results = await asyncio.gather(*index_tasks, return_exceptions=True)
        
        # 处理股票基础数据
        if isinstance(get_stock_basic, Exception) or get_stock_basic is None:
            logger.error("ods_stock_basic数据库数据导入失败, 数据为空或异常")
            failure_list.append("ods_stock_basic")
        else:
            data_load.append("ods_stock_basic", get_stock_basic)
        
        # 处理日线行情数据
        if isinstance(get_daily, Exception) or get_daily is None:
            logger.error(f"ods_daily数据库数据导入失败, 数据为空或异常, 交易日: {get_trade_cal['cal_date'].values[0]}")
            failure_list.append("ods_daily")
        else:
            data_load.append("ods_daily", get_daily)
        
        # 处理每日指标数据
        if isinstance(get_daily_basic, Exception) or get_daily_basic is None:
            logger.error(f"ods_daily_basic数据库数据导入失败, 数据为空或异常, 交易日: {get_trade_cal['cal_date'].values[0]}")
            failure_list.append("ods_daily_basic")
        else:
            data_load.append("ods_daily_basic", get_daily_basic)
        
        # 处理指数日线行情数据
        for i, get_index_daily in enumerate(index_results):
            if isinstance(get_index_daily, Exception) or get_index_daily is None:
                logger.error(f"ods_index_daily数据库数据导入失败, 数据为空或异常, 指数代码: {index_code[i]}, 交易日: {get_trade_cal['cal_date'].values[0]}")
                failure_list.append("ods_index_daily")
                continue
            data_load.append("ods_index_daily", get_index_daily)
        
        # 处理资金流向数据
        if isinstance(get_moneyflow, Exception) or get_moneyflow is None:
            logger.error(f"ods_moneyflow数据库数据导入失败, 数据为空或异常, 交易日: {get_trade_cal['cal_date'].values[0]}")
            failure_list.append("ods_moneyflow")
        else:
            data_load.append("ods_moneyflow", get_moneyflow)
        
        # 处理股票涨跌幅数据
        if isinstance(get_stk_limit, Exception) or get_stk_limit is None:
            logger.error(f"ods_stk_limit数据库数据导入失败, 数据为空或异常, 交易日: {get_trade_cal['cal_date'].values[0]}")
            failure_list.append("ods_stk_limit")
        else:
            data_load.append("ods_stk_limit", get_stk_limit)
        
        trade_date = datetime.strptime(get_trade_cal['cal_date'].values[0], "%Y%m%d").strftime("%Y-%m-%d")
        
        # 同步执行dw_daily_trends操作
        logger.info("开始计算股票日趋势数据, 表dw_daily_trends")
        try:
            dw_daily_trends_daily(trade_date)
        except Exception as e:
            logger.error(f"dw_daily_trends_daily执行失败: {str(e)}")
            failure_list.append("dw_daily_trends")
        
        # 异步执行剩余的操作
        logger.info("开始计算日复盘数据, 表dm_daily_replay")
        dm_replay_task = asyncio.get_event_loop().run_in_executor(None, dm_daily_replay_daily, trade_date)
        
        logger.info("开始计算个股股性数据, 表dm_stock_performance")
        dm_performance_task = asyncio.get_event_loop().run_in_executor(None, dm_stock_performance_daily, trade_date)
        
        logger.info("开始计算每日涨停数据, 表dm_up_limit_statistics")
        dm_limit_task = asyncio.get_event_loop().run_in_executor(None, dm_up_limit_statistics_daily, trade_date)
        
        # 等待剩余的异步任务完成
        dm_results = await asyncio.gather(dm_replay_task, dm_performance_task, dm_limit_task, return_exceptions=True)
        
        # 处理剩余操作的结果
        dm_replay_result, dm_performance_result, dm_limit_result = dm_results
        
        if isinstance(dm_replay_result, Exception):
            logger.error(f"dm_daily_replay_daily执行失败: {str(dm_replay_result)}")
            failure_list.append("dm_daily_replay")
        
        if isinstance(dm_performance_result, Exception):
            logger.error(f"dm_stock_performance_daily执行失败: {str(dm_performance_result)}")
            failure_list.append("dm_stock_performance")
        
        if isinstance(dm_limit_result, Exception):
            logger.error(f"dm_up_limit_statistics_daily执行失败: {str(dm_limit_result)}")
            failure_list.append("dm_up_limit_statistics")
        
        data_load.close()
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