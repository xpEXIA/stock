import ast
from datetime import datetime, timedelta
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger
from django.http import request, JsonResponse

from stockDataETL.run.AsyncDailyTask import asyncDailyTask
from asyncio import get_event_loop


def initDatabase(request):
    """
    重写初始化数据库函数，通过调用AsyncDailyTask函数来实现对过去一年交易日的数据库初始化
    """
    logger.info("开始初始化数据库")
    
    # 创建GetTSData实例获取交易日历
    get_TS_data = GetTSData()
    
    # 计算过去一年的日期范围
    end_date = datetime.today().strftime("%Y%m%d")
    start_date = (datetime.today() - timedelta(days=365)).strftime("%Y%m%d")
    
    # 获取过去一年的交易日历数据
    logger.info(f"获取{start_date}至{end_date}的交易日历数据")
    get_trade_cal = get_TS_data.getTradeCal(start_date=start_date, end_date=end_date)
    
    if get_trade_cal is None:
        logger.error("获取交易日历数据失败")
        return JsonResponse(
            {
                "status": "error",
                "message": "获取交易日历数据失败"
            }
        )
    
    # 筛选出is_open为1的交易日期
    logger.info("筛选交易日")
    trade_date_list = get_trade_cal[(get_trade_cal["is_open"] == 1) &
                                    (get_trade_cal["cal_date"] <= end_date)]["cal_date"].tolist()
    
    logger.info(f"共获取到{len(trade_date_list)}个交易日")
    logger.info(f"交易日列表: {trade_date_list}")
    
    # 遍历每个交易日，调用AsyncDailyTask函数
    failure_dates = []
    for trade_date in trade_date_list:
        logger.info(f"开始处理交易日: {trade_date}")
        
        try:
            # 创建模拟请求对象，包含trade_date参数
            class MockRequest:
                def __init__(self, trade_date):
                    self.GET = {'date': trade_date}
            
            # 调用AsyncDailyTask函数处理该交易日数据
            loop = get_event_loop()
            result = loop.run_until_complete(asyncDailyTask(MockRequest(trade_date)))
            
            # 检查处理结果
            if result.status_code == 200:
                logger.info(f"交易日{trade_date}数据处理成功")
            else:
                logger.error(f"交易日{trade_date}数据处理失败: {result.content.decode('utf-8')}")
                failure_dates.append(trade_date)
                
        except Exception as e:
            logger.error(f"处理交易日{trade_date}时发生异常: {str(e)}")
            failure_dates.append(trade_date)
    
    if not failure_dates:
        logger.info("数据库初始化完成")
        return JsonResponse(
            {
                "status": "success",
                "message": f"数据库初始化完成，共处理{len(trade_date_list)}个交易日"
            }
        )
    else:
        logger.error(f"数据库初始化失败，失败日期: {failure_dates}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"数据库初始化失败，{len(failure_dates)}个交易日处理失败，失败日期: {failure_dates}"
            }
        )
