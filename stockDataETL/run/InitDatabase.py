from stockDataETL.scripts.DataExtract import GetTSData
from stockDataETL.scripts.DataLoad import DataLoad
from stockDataETL import logging


def initDatabase():

    logging.info("开始初始化数据库")
    get_TS_data = GetTSData()
    data_load = DataLoad()

    logging.info("开始初始化股票基础数据, 表ods_stock_basic")
    get_stock_basic = get_TS_data.getStockBasic()
    data_load.append("ods_stock_basic", get_stock_basic)

    logging.info("开始初始化交易日历数据, 表ods_trade_cal")
    get_trade_cal = get_TS_data.getTradeCal()
    data_load.append("ods_trade_cal", get_trade_cal)

    logging.info("开始初始化股票公司数据, 表ods_stock_company")
    get_stock_company = get_TS_data.getStockCompany()
    data_load.append("ods_stock_company", get_stock_company)

    logging.info("开始初始化日线行情数据, 表ods_daily")
    get_daily = get_TS_data.getDaily()
    data_load.append("ods_daily", get_daily)
