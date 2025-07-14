from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logging


def initDatabase():

    logging.info("开始初始化数据库")
    get_TS_data = GetTSData()
    data_load = DataLoad()
    try:
        logging.info("开始清空数据")
        data_load.truncate("ods_stock_basic")
        data_load.truncate("ods_trade_cal")
        data_load.truncate("ods_stock_company")
        data_load.truncate("ods_daily")
    except:
        logging.error("清空数据库失败")
        return {"status": "error", "message": "清空数据库失败"}

    try:
        logging.info("开始初始化股票基础数据, 表ods_stock_basic")
        get_stock_basic = get_TS_data.getStockBasic()
        data_load.append("ods_stock_basic", get_stock_basic)
    except Exception as e:
        logging.error("ods_stock_basic数据库数据导入失败, 异常信息: ", exc_info=True)
        return {"status": "error", "message": "ods_stock_basic数据库数据导入失败"}

    try:
        logging.info("开始初始化交易日历数据, 表ods_trade_cal")
        get_trade_cal = get_TS_data.getTradeCal()
        data_load.append("ods_trade_cal", get_trade_cal)
    except Exception as e:
        logging.error("ods_trade_cal数据库数据导入失败, 错误信息: ", exc_info=True)
        return {"status": "error", "message": "ods_trade_cal数据库数据导入失败"}

    try:
        logging.info("开始初始化股票公司数据, 表ods_stock_company")
        get_stock_company = get_TS_data.getStockCompany()
        data_load.append("ods_stock_company", get_stock_company)
    except Exception as e:
        logging.error("ods_stock_company数据库数据导入失败, 错误信息: ", exc_info=True)
        return {"status": "error", "message": "ods_stock_company数据库数据导入失败"}

    try:
        logging.info("开始初始化日线行情数据, 表ods_daily")
        get_daily = get_TS_data.getDaily()
        data_load.append("ods_daily", get_daily)
    except Exception as e:
        logging.error("ods_daily数据库数据导入失败, 错误信息: ", exc_info=True)
        return {"status": "error", "message": "ods_daily数据库数据导入失败"}

