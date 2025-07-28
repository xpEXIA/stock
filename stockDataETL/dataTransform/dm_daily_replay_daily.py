from datetime import datetime
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.CommonUtils.get_pretrade_date import get_pretrade_date


def dm_daily_replay_daily(trade_date: str) -> str:

    load_data = DataLoad()
    logger.info(f"开始处理日复盘数据, 交易日:{trade_date}, 表dm_daily_replay")
    logger.info("获取交易日历数据")
    pretrade_date = get_pretrade_date(trade_date)

    logger.info("获取复盘计算数据")
    trade_date_data =  load_data.search(
        """
        select 
            pre_daily_trends.ts_code as ts_code,
            pre_daily_trends.close as pre_close,
            pre_daily_trends.up_limit as pre_up_limit,
            daily_trends.open as open,
            daily_trends.close as close
        from (
                select 
                    ts_code,
                    close,
                    up_limit
                from dw_daily_trends
                where trade_date = :pretrade_date and pct_chg >= 0.05
             ) pre_daily_trends 
            left join (
                select 
                    ts_code,
                    open,
                    close
                from dw_daily_trends
                where trade_date = :trade_date
            ) daily_trends on pre_daily_trends.ts_code = daily_trends.ts_code
        """,
        {
            "trade_date": trade_date,
            "pretrade_date": datetime.strptime(pretrade_date, "%Y%m%d").strftime("%Y-%m-%d")
        }
    )
    trade_date_data = DataFrame(trade_date_data, columns=[
        "ts_code", "pre_close", "pre_up_limit", "open", "close"
    ])

    logger.info(f"开始计算日复盘数据, 交易日{trade_date}")
    dm_daily_replay_data = {}
    dm_daily_replay_data["trade_date"] = trade_date
    up_limit_data = trade_date_data[trade_date_data["pre_up_limit"] == trade_date_data["pre_close"]]
    dm_daily_replay_data["last_up_limit"] = up_limit_data["ts_code"].count()
    dm_daily_replay_data["last_up_limit_open_up"] = up_limit_data[trade_date_data["open"] > 0]["ts_code"].count()
    dm_daily_replay_data["last_up_limit_close_up"] = up_limit_data[trade_date_data["close"] > 0]["ts_code"].count()
    dm_daily_replay_data["last_up_limit_open_up_5"] = up_limit_data[trade_date_data["open"] >= 0.05]["ts_code"].count()
    dm_daily_replay_data["last_up_limit_close_up_5"] = up_limit_data[trade_date_data["close"] >= 0.05]["ts_code"].count()
    last_up_5 = trade_date_data[trade_date_data["pre_close"] >= 0.05]
    dm_daily_replay_data["last_up_5"] =last_up_5["ts_code"].count()
    dm_daily_replay_data["last_up_5_open_up"] = last_up_5[trade_date_data["open"] > 0]["ts_code"].count()
    dm_daily_replay_data["last_up_5_close_up"] = last_up_5[trade_date_data["close"] > 0]["ts_code"].count()
    dm_daily_replay_data["last_up_5_open_up_5"] = last_up_5[trade_date_data["open"] >= 0.05]["ts_code"].count()
    dm_daily_replay_data["last_up_5_close_up_5"] = last_up_5[trade_date_data["close"] >= 0.05]["ts_code"].count()
    dm_daily_replay_data = DataFrame(dm_daily_replay_data, index=[0])
    load_data.append("dm_daily_replay",dm_daily_replay_data)

    load_data.close()

