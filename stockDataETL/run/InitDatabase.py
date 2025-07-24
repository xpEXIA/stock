import ast
from datetime import datetime, timedelta
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger
from django.http import request, JsonResponse

def initDatabase(request):

    # table_list = request.GET.get("table_list")
    # table_list = ast.literal_eval(table_list)
    # if table_list is None:
    table_list = ["ods_stock_basic", "ods_trade_cal", "ods_stock_company", "ods_daily", "ods_daily_basic",
                  "ods_index_basic", "ods_index_daily", "ods_moneyflow", "ods_stk_limit", 'dw_daily_trends',
                  "dm_daily_replay", "dm_stock_performance", "dm_up_limit_statistics"]

    logger.info("开始初始化数据库")
    get_TS_data = GetTSData()
    data_load = DataLoad()
    failure_list = []

    logger.info("开始清空数据")
    for table in table_list:
        data_load.truncate(table)

    logger.info("开始初始化股票基础数据, 表ods_stock_basic")
    get_stock_basic = get_TS_data.getStockBasic()
    if get_stock_basic.empty:
        logger.error("ods_stock_basic数据库数据导入失败, 数据为空")
        failure_list.append("ods_stock_basic")
    data_load.append("ods_stock_basic", get_stock_basic)

    logger.info("开始初始化交易日历数据, 表ods_trade_cal")
    start_date = (datetime.today() - timedelta(year=1)).strftime("%Y%m%d")
    end_date = (datetime.today() + timedelta(month=6).strftime("%Y%m%d"))
    get_trade_cal = get_TS_data.getTradeCal(start_date=start_date, end_date=end_date)
    if get_trade_cal.empty:
        logger.error("ods_trade_cal数据库数据导入失败, 数据为空")
        failure_list.append("ods_trade_cal")
    data_load.append("ods_trade_cal", get_trade_cal)

    logger.info("开始初始化股票公司数据, 表ods_stock_company")
    get_stock_company = get_TS_data.getStockCompany()
    if get_stock_company.empty:
        logger.error("ods_stock_company数据库数据导入失败, 数据为空")
        failure_list.append("ods_stock_company")
    data_load.append("ods_stock_company", get_stock_company)

    logger.info("获取初始化交易日志列表")
    trade_date_list = get_trade_cal[(get_trade_cal["is_open"] == 1) &
                                    (get_trade_cal["cal_date"] <= datetime.today().strftime("%Y%m%d"))]["cal_date"].tolist()

    logger.info("开始初始化日线行情数据, 表ods_daily")
    for trade_date in trade_date_list:
        get_daily = get_TS_data.getDaily(trade_date=trade_date)
        if get_daily.empty:
            logger.error(f"ods_daily数据库数据导入失败, 数据为空, 交易日: {trade_date}")
            failure_list.append("ods_daily")
            continue
        data_load.append("ods_daily", get_daily)

    logger.info("开始初始化每日指标数据, 表ods_daily_basic")
    for trade_date in trade_date_list:
        get_daily_basic = get_TS_data.getDailyBasic(trade_date=trade_date)
        if get_daily_basic.empty:
            logger.error(f"ods_daily_basic数据库数据导入失败, 数据为空, 交易日: {trade_date}")
            failure_list.append("ods_daily_basic")
            continue
        data_load.append("ods_daily_basic", get_daily_basic)

    logger.info("开始初始化指数基础数据, 表ods_index_basic")
    market = ["SZSE", "SSE"]
    for market_ in market:
        get_index_basic = get_TS_data.getIndexBasic(market=market_)
        if get_index_basic.empty:
            logger.error(f"ods_index_basic数据库数据导入失败, 数据为空, 交易所: {market_}")
            failure_list.append("ods_index_basic")
            continue
        data_load.append("ods_index_basic", get_index_basic)

    logger.info("开始初始化指数日线行情数据, 表ods_index_daily")
    index_code = ["000001.SH","399107.SZ"]
    for index_code_ in index_code:
        get_index_daily = get_TS_data.getIndexDaily(ts_code=index_code_,
                                                    start_date=start_date,
                                                    end_date=datetime.today().strftime("%Y%m%d"))
        if get_index_daily.empty:
            logger.error(f"ods_index_daily数据库数据导入失败, 数据为空, 指数代码: {index_code_}")
            failure_list.append("ods_index_daily")
            continue
        data_load.append("ods_index_daily", get_index_daily)

    logger.info("开始初始化资金流向数据, 表ods_moneyflow")
    for trade_date in trade_date_list:
        get_moneyflow = get_TS_data.getMoneyFlow(trade_date=trade_date)
        if get_moneyflow.empty:
            logger.error(f"ods_moneyflow数据库数据导入失败, 数据为空, 交易日: {trade_date}")
            failure_list.append("ods_moneyflow")
            continue
        data_load.append("ods_moneyflow", get_moneyflow)

    logger.info("开始初始化股票涨跌幅数据, 表ods_stk_limit")
    for trade_date in trade_date_list:
        get_stk_limit = get_TS_data.getStkLimit(trade_date=trade_date)
        if get_stk_limit.empty:
            logger.error(f"ods_stk_limit数据库数据导入失败, 数据为空, 交易日: {trade_date}")
            failure_list.append("ods_stk_limit")
            continue
        data_load.append("ods_stk_limit", get_stk_limit)

    data_load.close()
    if failure_list == []:
        logger.info("初始化数据库成功")
        return JsonResponse(
            {
                "status": "success",
                "message": "初始化数据库成功"
            }
        )
    logger.error(f"初始化数据库失败, 失败表: {failure_list}")
    return JsonResponse(
        {
            "status": "error",
            "message": f"初始化数据库失败, 失败表: {failure_list}"
        }
    )