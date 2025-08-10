from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_daily_task(trade_date: str, connect: object = DataLoad()) -> str:


    """
    股票日线行情数据任务
    """

    data_load = connect
    get_TS_data = GetTSData()

    logger.info("开始获取日线行情数据, 表ods_daily")
    get_daily = get_TS_data.getDaily(trade_date=trade_date)
    if get_daily is None:
        logger.error(f"ods_daily数据库数据导入失败, 数据为空, 交易日: {trade_date}")
        return 'ods_daily'

    data_load.append("ods_daily", get_daily)
    data_load.close()

