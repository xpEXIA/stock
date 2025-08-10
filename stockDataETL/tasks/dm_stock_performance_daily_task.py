from stockDataETL import logger
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataLoad.DataLoad import DataLoad


def dm_stock_performance_daily_task(trade_date: str,
                            connect: object = DataLoad()) -> None:
    
    logger.info("开始计算日复盘数据, dm_stock_performance")
    try:
        dm_stock_performance_daily(trade_date=trade_date, connect=connect)
        connect.close()
    except Exception as e:
        connect.close()
        logger.error(f"dm_stock_performance_daily执行失败: {str(e)}")
        return "dm_stock_performance"


