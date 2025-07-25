from datetime import datetime
from stockDataETL.dataExtract.GetTSData import GetTSData
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL import logger
from django.http import request, JsonResponse
from stockDataETL.dataTransform.dm_daily_replay_daily import dm_daily_replay_daily
from stockDataETL.dataTransform.dm_stock_performance_daily import dm_stock_performance_daily
from stockDataETL.dataTransform.dm_up_limit_statistics_daily import dm_up_limit_statistics_daily
from stockDataETL.dataTransform.dw_daily_trends_daily import dw_daily_trends_daily


def dailyTask(request):

    get_TS_data = GetTSData()
    data_load = DataLoad()
    failure_list = []
    logger.info("开始获取ods数据")

    today = datetime.today().strftime("%Y%m%d")
    get_trade_cal = get_TS_data.getTradeCal(start_date=today, end_date=today)
    if get_trade_cal['is_open'] != 1:
        data_load.close()
        return JsonResponse(
            {
                "status": "error",
                "message": "非交易日"
            }
        )
    else:

        logger.info("开始更新股票基础数据, 表ods_stock_basic")
        data_load.truncate("ods_stock_basic")
        get_stock_basic = get_TS_data.getStockBasic()
        if get_stock_basic.empty:
            logger.error("ods_stock_basic数据库数据导入失败, 数据为空")
            failure_list.append("ods_stock_basic")
        data_load.append("ods_stock_basic", get_stock_basic)

        logger.info("开始获取日线行情数据, 表ods_daily")
        get_daily = get_TS_data.getDaily(trade_date=get_trade_cal['cal_date'])
        if get_daily.empty:
            logger.error(f"ods_daily数据库数据导入失败, 数据为空, 交易日: {get_trade_cal['cal_date']}")
            failure_list.append("ods_daily")
        data_load.append("ods_daily", get_daily)

        logger.info("开始获取每日指标数据, 表ods_daily_basic")
        get_daily_basic = get_TS_data.getDailyBasic(trade_date=get_trade_cal['cal_date'])
        if get_daily_basic.empty:
            logger.error(f"ods_daily_basic数据库数据导入失败, 数据为空, 交易日: {get_trade_cal['cal_date']}")
            failure_list.append("ods_daily_basic")
        data_load.append("ods_daily_basic", get_daily_basic)

        logger.info("开始获取指数日线行情数据, 表ods_index_daily")
        index_code = ["000001.SH", "399107.SZ"]
        for index_code_ in index_code:
            get_index_daily = get_TS_data.getIndexDaily(ts_code=index_code_,trade_date=get_trade_cal['cal_date'])
            if get_index_daily.empty:
                logger.error(f"ods_index_daily数据库数据导入失败, 数据为空, 指数代码: {index_code_}, 交易日: {get_trade_cal['cal_date']}")
                failure_list.append("ods_index_daily")
                continue
            data_load.append("ods_index_daily", get_index_daily)

        logger.info("开始获取资金流向数据, 表ods_moneyflow")
        get_moneyflow = get_TS_data.getMoneyFlow(trade_date=get_trade_cal['cal_date'])
        if get_moneyflow.empty:
            logger.error(f"ods_moneyflow数据库数据导入失败, 数据为空, 交易日: {get_trade_cal['cal_date']}")
            failure_list.append("ods_moneyflow")
        data_load.append("ods_moneyflow", get_moneyflow)

        logger.info("开始获取股票涨跌幅数据, 表ods_stk_limit")
        get_stk_limit = get_TS_data.getStkLimit(trade_date=get_trade_cal['cal_date'])
        if get_stk_limit.empty:
            logger.error(f"ods_stk_limit数据库数据导入失败, 数据为空, 交易日: {get_trade_cal['cal_date']}")
            failure_list.append("ods_stk_limit")
        data_load.append("ods_stk_limit", get_stk_limit)

        trade_date = datetime.strptime(get_trade_cal['cal_date'], "%Y%m%d").strftime("%Y-%m-%d")
        logger.info("开始计算股票日趋势数据, 表dw_daily_trends")
        try:
            dw_daily_trends_daily(trade_date)
        except Exception as e:
            logger.error(f"dw_daily_trends_daily执行失败: {str(e)}")
            failure_list.append("dw_daily_trends")

        logger.info("开始计算日复盘数据, 表dm_daily_replay")
        try:
            dm_daily_replay_daily(trade_date)
        except Exception as e:
            logger.error(f"dm_daily_replay_daily执行失败: {str(e)}")
            failure_list.append("dm_daily_replay")

        logger.info("开始计算个股股性数据, 表dm_stock_performance")
        try:
            dm_stock_performance_daily(trade_date)
        except Exception as e:
            logger.error(f"dm_stock_performance_daily执行失败: {str(e)}")
            failure_list.append("dm_stock_performance")

        logger.info("开始计算每日涨停数据, 表dm_up_limit_statistics")
        try:
            dm_up_limit_statistics_daily(trade_date)
        except Exception as e:
            logger.error(f"dm_up_limit_statistics_daily执行失败: {str(e)}")
            failure_list.append("dm_up_limit_statistics")

        data_load.close()
        if failure_list == []:
            logger.info("每日数据获取成功")
            return JsonResponse(
                {
                    "status": "success",
                    "message": "每日数据获取成功"
                }
            )
        logger.error(f"每日数据获取失败, 失败表: {failure_list}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"每日数据获取失败, 失败表: {failure_list}"
            }
        )