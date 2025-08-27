from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_moneyflow_task(trade_date: str) -> str:


    """
    股票资金流向数据任务
    """

    data_load = DataLoad()
    get_TS_data = GetTSData()

    logger.info("开始获取资金流向数据, 表ods_moneyflow")
    get_moneyflow = get_TS_data.getMoneyFlow(trade_date=trade_date)
    if get_moneyflow is None:
        logger.error(f"ods_moneyflow数据库数据导入失败, 数据为空, 交易日: {trade_date}")
        return "ods_moneyflow"
    
    data_load.append("ods_moneyflow", get_moneyflow)
    data_load.close()
