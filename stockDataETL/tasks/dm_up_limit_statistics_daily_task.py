from stockDataETL import logger
from stockDataETL.dataTransform.dm_up_limit_statistics_daily import dm_up_limit_statistics_daily
from stockDataETL.dataLoad.DataLoad import DataLoad


def dm_up_limit_statistics_daily_task(trade_date: str,
                            connect: object = DataLoad()) -> None:
    
    logger.info("开始计算日复盘数据, dm_up_limit_statistics")
    try:
        dm_up_limit_statistics_daily(trade_date, connect)
        connect.close()
    except Exception as e:
        connect.close()
        logger.error(f"dm_up_limit_statistics_daily执行失败: {str(e)}")
        return "dm_up_limit_statistics"


