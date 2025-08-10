from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_index_daily_task(trade_date: str, connect: object = DataLoad()) -> list:


    """
    股票指数日线行情数据任务
    """

    data_load = connect
    get_TS_data = GetTSData()
    failure_list = []

    logger.info("开始获取指数日线行情数据, 表ods_index_daily")
    index_code = ["000001.SH", "399107.SZ"]
    for index_code_ in index_code:
        get_index_daily = get_TS_data.getIndexDaily(ts_code=index_code_,trade_date=trade_date)
        if get_index_daily is None:
            logger.error(f"ods_index_daily数据库数据导入失败, 数据为空, 指数代码: {index_code_}, 交易日: {trade_date}")
            failure_list.append("index_code_")
            continue
        data_load.append("ods_index_daily", get_index_daily)

    data_load.close()   
    if len(failure_list) > 0:
        return 'ods_index_daily'

