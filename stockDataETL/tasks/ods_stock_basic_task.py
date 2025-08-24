from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger

def ods_stock_basic_task(connect: object = DataLoad()) -> str:

    """
    股票基础数据任务
    """

    data_load = connect
    get_TS_data = GetTSData()

    logger.info("开始更新股票基础数据, 表ods_stock_basic")
    data_load.truncate("ods_stock_basic")
    get_stock_basic = get_TS_data.getStockBasic()
    if get_stock_basic is None:
        logger.error("ods_stock_basic数据库数据导入失败, 数据为空")
        return 'ods_stock_basic'

    data_load.append("ods_stock_basic", get_stock_basic)

