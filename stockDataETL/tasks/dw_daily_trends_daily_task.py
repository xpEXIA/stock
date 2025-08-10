from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily
from stockDataETL.dataLoad.DataLoad import DataLoad


def dw_daily_trends_daily_task(trade_date: str,
                            connect: object = DataLoad()) -> None:
    
    logger.info("开始计算日复盘数据, dw_daily_trends")
    try:
        dw_daily_trends_daily(trade_date=trade_date, connect=connect)
        connect.close()
    except Exception as e:
        connect.close()
        logger.error(f"dw_daily_trends_daily执行失败: {str(e)}")
        return "dw_daily_trends"


