from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_stk_limit_task(trade_date: str, connect: object = DataLoad()) -> str:


    """
    股票资金流向数据任务
    """

    data_load = connect
    get_TS_data = GetTSData()

    logger.info("开始获取股票涨跌幅数据, 表ods_stk_limit")
    get_stk_limit = get_TS_data.getStkLimit(trade_date=trade_date)
    if get_stk_limit is None:
        logger.error(f"ods_stk_limit数据库数据导入失败, 数据为空, 交易日: {trade_date}")
        return "ods_stk_limit"

    data_load.append("ods_stk_limit", get_stk_limit)
    data_load.close()