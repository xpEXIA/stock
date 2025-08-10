from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataLoad.DataLoad import DataLoad


def dm_daily_replay_daily_task(trade_date: str,
                            connect: object = DataLoad()) -> None:
    
    logger.info("开始计算日复盘数据, 表dm_daily_replay")
    try:
        dm_daily_replay_daily(trade_date, connect)
        connect.close()
    except Exception as e:
        connect.close()
        logger.error(f"dm_daily_replay_daily执行失败: {str(e)}")
        return "dm_daily_replay"


