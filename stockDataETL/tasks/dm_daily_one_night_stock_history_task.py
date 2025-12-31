from stockDataETL import logger
from stockDataETL.dataTransform.dm_daily_one_night_stock_history import dm_daily_one_night_stock_history


def dm_daily_one_night_stock_history_task(start_time: str, end_time: str, period: int = 5) -> None:
    """
    执行一夜持股历史数据计算任务
    
    Args:
        start_time: 开始时间，例如：'20240101' 或'20241231235959'
        end_time: 结束时间，例如：'20240101' 或'20241231235959'
        period: 历史成交量计算周期，默认为过去5个交易日
    """
    logger.info("开始计算一夜持股历史数据, 表dm_daily_one_night_stock")
    try:
        dm_daily_one_night_stock_history(start_time, end_time, period)
        logger.info("一夜持股历史数据计算完成")
    except Exception as e:
        logger.error(f"dm_daily_one_night_stock_history执行失败: {str(e)}")
        raise e
