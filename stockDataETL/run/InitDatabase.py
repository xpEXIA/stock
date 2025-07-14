from datetime import datetime
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logging, engine


def initDatabase(table_list=["ods_stock_basic", "ods_trade_cal", "ods_stock_company", "ods_daily", "ods_daily_basic",
                             "ods_index_basic", "ods_index_daily", "ods_moneyflow", "ods_stk_limit", 'dw_daily_trends',
                             "dm_daily_replay", "dm_stock_performance", "dm_up_limit_statistics"]):

    logging.info("开始初始化数据库")
    get_TS_data = GetTSData()
    data_load = DataLoad()

    logging.info("开始清空数据")
    for table in table_list:
        data_load.truncate(table)

    try:
        logging.info("开始初始化股票基础数据, 表ods_stock_basic")
        get_stock_basic = get_TS_data.getStockBasic()
        data_load.append("ods_stock_basic", get_stock_basic)
    except Exception as e:
        logging.error("ods_stock_basic数据库数据导入失败, 异常信息: ", exc_info=True)
        return {"status": "error", "message": "ods_stock_basic数据库数据导入失败"}

    try:
        logging.info("开始初始化交易日历数据, 表ods_trade_cal")
        start_date = datetime.today().strftime("%Y%m%d")
        end_date = (datetime.today()
                    .replace(year=datetime.strptime(start_date, "%Y%m%d").year + 1)
                    .strftime("%Y%m%d"))
        get_trade_cal = get_TS_data.getTradeCal(start_date=start_date, end_date=end_date)
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
        with engine.connect() as conn:
            trade_date_list = conn.execute("select max(trade_date) from ods_trade_cal").fetchone()[0]
        get_daily = get_TS_data.getDaily(trade_date=trade_date)
        data_load.append("ods_daily", get_daily)
    except Exception as e:
        logging.error("ods_daily数据库数据导入失败, 错误信息: ", exc_info=True)
        return {"status": "error", "message": "ods_daily数据库数据导入失败"}

