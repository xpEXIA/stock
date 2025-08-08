from datetime import datetime, timedelta
from os import rename
import pandas as pd
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.commonUtils.trade_date_complete_check import trade_date_complete_check


def dm_stock_performance_daily(trade_date: str,
                               interval: int = 180,
                               connect: object = DataLoad()) -> None:

    data_load = connect
    logger.info(f"开始处理股性数据, 交易日:{trade_date}, 表dm_stock_performance")
    data = data_load.search(
        """
        select 
            trade_date,
            ts_code,
            name,
            industry,
            close,
            open_pct_chg,
            high_pct_chg,
            pct_chg,
            up_limit,
            down_limit
        from dw_daily_trends
        where trade_date >= date_sub(:trade_date, interval :interval day)
        """,
        {
            "trade_date": trade_date,
            "interval": interval
        }
    )
    data = DataFrame(data, columns=[
        "trade_date", "ts_code", "name", "industry","close",
        "open_pct_chg", "high_pct_chg","pct_chg","up_limit","down_limit",
    ])

    up_days = data[data['pct_chg'] > 0].groupby(["ts_code", "name", "industry"]).agg(
        up_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "up_days"})
    down_days = data[data['pct_chg'] <= 0].groupby(["ts_code", "name", "industry"]).agg(
        down_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "down_days"})
    up_5_days = data[data['pct_chg'] >= 5].groupby(["ts_code", "name", "industry"]).agg(
        up_5_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "up_5_days"})
    down_5_days = data[data['pct_chg'] <= -5].groupby(["ts_code", "name", "industry"]).agg(
        down_5_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "down_5_days"})
    up_limit_days = data[data['close'] == data['up_limit']].groupby(["ts_code", "name", "industry"]).agg(
        up_limit_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "up_limit_days"})
    down_limit_days = data[data['close'] == data['down_limit']].groupby(["ts_code", "name", "industry"]).agg(
        down_limit_days=("pct_chg", "count")
    ).reset_index().rename(columns={"pct_chg": "down_limit_days"})

    def _cal_pct_chg(data,up_data,cal_col,cal_fun,groupby,merge_on):

        up_data["trade_date"] = up_data["trade_date"].apply(
            # lambda x: (
            #     datetime.strptime(x, "%Y-%m-%d") + timedelta(days=1)
            # ).strftime("%Y-%m-%d")
            lambda x: (x + timedelta(days=1))
        )
        return pd.merge(
            up_data,
            data,
            how="left",
            on=merge_on
        ).groupby([groupby]).agg(
            cal_col=(cal_col, cal_fun)
        ).reset_index()

    up_5_data = data[data['pct_chg'] >= 5][["trade_date", "ts_code"]]
    up_5_next_open_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "open_pct_chg"]],
        up_5_data,
        "open_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_5_next_open_pct_chg"})

    up_5_next_high_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "high_pct_chg"]],
        up_5_data,
        "high_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_5_next_high_pct_chg"})

    up_5_next_close_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "pct_chg"]],
        up_5_data,
        "pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_5_next_close_pct_chg"})

    up_limit_data = data[data['close'] == data['up_limit']][["trade_date", "ts_code"]]
    up_limit_next_open_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "open_pct_chg"]],
        up_limit_data,
        "open_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_limit_next_open_pct_chg"})

    up_limit_next_high_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "high_pct_chg"]],
        up_limit_data,
        "high_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_limit_next_high_pct_chg"})

    up_limit_next_close_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "pct_chg"]],
        up_limit_data,
        "pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"]
    ).rename(columns={"cal_col": "up_limit_next_close_pct_chg"})

    trade_days = (data.groupby('ts_code')['pct_chg'].count().
                  reset_index().rename(columns={"pct_chg": "trade_days"}))

    dm_stock_performance_data = up_days.merge(
        down_days,
        how="left",
        on=["ts_code", "name", "industry"]
    ).merge(
        up_5_days,
        how="left",
        on=["ts_code", "name", "industry"]
    ).merge(
        down_5_days,
        how="left",
        on=["ts_code", "name", "industry"]
    ).merge(
        up_limit_days,
        how="left",
        on=["ts_code", "name", "industry"]
    ).merge(
        down_limit_days,
        how="left",
        on=["ts_code", "name", "industry"]
    ).merge(
        up_5_next_open_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        up_5_next_high_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        up_5_next_close_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        up_limit_next_open_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        up_limit_next_high_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        up_limit_next_close_pct_chg,
        how="left",
        on=["ts_code"]
    ).merge(
        trade_days,
        how="left",
        on=["ts_code"]
    )
    dm_stock_performance_data["trade_date"] = trade_date
    dm_stock_performance_data.fillna(0, inplace=True)
    dm_stock_performance_data[
        [
            "up_5_next_open_pct_chg",
            "up_5_next_high_pct_chg",
            "up_5_next_close_pct_chg",
            "up_limit_next_open_pct_chg",
            "up_limit_next_high_pct_chg",
            "up_limit_next_close_pct_chg",
        ]
    ] = dm_stock_performance_data[
        [
            "up_5_next_open_pct_chg",
            "up_5_next_high_pct_chg",
            "up_5_next_close_pct_chg",
            "up_limit_next_open_pct_chg",
            "up_limit_next_high_pct_chg",
            "up_limit_next_close_pct_chg",
        ]
    ].apply(lambda x: round(x, 4))

    data_load.append("dm_stock_performance", dm_stock_performance_data)







