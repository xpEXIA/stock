from stockDataETL import logger
from stockDataETL.dataTransform.dm_daily_one_night_stock import dm_daily_one_night_stock


def dm_daily_one_night_stock_daily_task(trade_date: str) -> None:
    """
    执行当日一夜情股票数据计算任务
    
    Args:
        trade_date: 交易日，格式如'2025-12-05'
    """
    logger.info("开始计算当日一夜持股数据, 表dm_daily_one_night_stock")
    try:
        dm_daily_one_night_stock(trade_date)
        logger.info("当日一夜持股数据计算完成")
    except Exception as e:
        logger.error(f"dm_daily_one_night_stock执行失败: {str(e)}")
        raise Exception(f"{trade_date}非交易日")
