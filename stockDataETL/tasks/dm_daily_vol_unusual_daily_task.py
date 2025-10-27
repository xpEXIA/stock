from stockDataETL import logger
from stockDataETL.dataTransform.dm_daily_vol_unusual import dm_daily_vol_unusual


def dm_daily_vol_unusual_daily_task(trade_date: str) -> None:

    logger.info("开始计算日复盘数据, dm_daily_vol_unusual")
    try:
        dm_daily_vol_unusual(trade_date)
    except Exception as e:
        logger.error(f"dm_daily_vol_unusual执行失败: {str(e)}")
        return "dm_daily_vol_unusual"


