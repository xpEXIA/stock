from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_daily_basic_task(trade_date: str) -> str:


    """
    股票每日指标数据任务
    """

    data_load = DataLoad()
    get_TS_data = GetTSData()

    logger.info("开始获取每日指标数据, 表ods_daily_basic")
    get_daily_basic = get_TS_data.getDailyBasic(trade_date=trade_date)
    if get_daily_basic is None:
        logger.error(f"ods_daily_basic数据库数据导入失败, 数据为空, 交易日: {trade_date}")
        return 'ods_daily_basic'

    data_load.append("ods_daily_basic", get_daily_basic)

