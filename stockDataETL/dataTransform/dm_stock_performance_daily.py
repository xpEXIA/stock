from datetime import datetime, timedelta
from os import rename
from tkinter.constants import ROUND

import pandas as pd
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.commonUtils.get_pretrade_date import get_pretrade_date
from stockDataETL.dataTransform.commonUtils.trade_date_complete_check import trade_date_complete_check


def dm_stock_performance_daily(
        trade_date: str,
        interval: int = 180
) -> None:

    data_load = DataLoad()
    logger.info(f"开始处理股性数据, 交易日:{trade_date}, 表dm_stock_performance")
    data = data_load.search(
        """
        select 
            trade_date,
            ts_code,
            name,
            industry,
            close,
            vol,
            open_pct_chg,
            high_pct_chg,
            pct_chg,
            up_limit,
            down_limit
        from dw_daily_trends
        where trade_date >= date_sub(:trade_date, interval :interval day) and trade_date <= :trade_date
        """,
        {
            "trade_date": trade_date,
            "interval": interval
        }
    )
    data = DataFrame(data, columns=[
        "trade_date", "ts_code", "name", "industry","close","vol",
        "open_pct_chg", "high_pct_chg","pct_chg","up_limit","down_limit",
    ])
    data['trade_date'] = data['trade_date'].apply(lambda x: x.strftime("%Y-%m-%d"))

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

    pretrade_date = get_pretrade_date(trade_date, interval)
    pretrade_date["cal_date"] = (pretrade_date["cal_date"]
                     .apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")))
    pretrade_date["pretrade_date"] = (pretrade_date["pretrade_date"]
                     .apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d")))
    def _cal_pct_chg(
            data,
            up_data,
            cal_col,
            cal_fun,
            groupby,
            merge_on,
            pretrade_date
        ):

        pretrade_date=pretrade_date
        up_data["trade_date"] = up_data["trade_date"].apply(
            # lambda x: (
            #     datetime.strptime(x, "%Y-%m-%d") + timedelta(days=1)
            # ).strftime("%Y-%m-%d")
            lambda x: (pretrade_date["pretrade_date"][pretrade_date["cal_date"] == x].values[0])
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
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_5_next_open_pct_chg"})

    up_5_next_high_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "high_pct_chg"]],
        up_5_data,
        "high_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_5_next_high_pct_chg"})

    up_5_next_close_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "pct_chg"]],
        up_5_data,
        "pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_5_next_close_pct_chg"})

    up_limit_data = data[data['close'] == data['up_limit']][["trade_date", "ts_code"]]
    up_limit_next_open_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "open_pct_chg"]],
        up_limit_data,
        "open_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_limit_next_open_pct_chg"})

    up_limit_next_high_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "high_pct_chg"]],
        up_limit_data,
        "high_pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_limit_next_high_pct_chg"})

    up_limit_next_close_pct_chg = _cal_pct_chg(
        data[["trade_date", "ts_code", "pct_chg"]],
        up_limit_data,
        "pct_chg",
        "mean",
        "ts_code",
        ["trade_date", "ts_code"],
        pretrade_date
    ).rename(columns={"cal_col": "up_limit_next_close_pct_chg"})

    trade_days = (data.groupby('ts_code')['pct_chg'].count().
                  reset_index().rename(columns={"pct_chg": "trade_days"}))

    trade_date_duplicate = (data[['trade_date','vol']]
                            .drop_duplicates(subset=['trade_date'],keep='first')
                            .sort_values(by='trade_date',ascending=False))
    vol_start_date = trade_date_duplicate['trade_date'].iloc[20]
    vol_end_date = trade_date_duplicate['trade_date'].iloc[1]
    mean_vol = (data[["ts_code", "vol"]][(data['trade_date'] >= vol_start_date)
                                                     & (data['trade_date'] <= vol_end_date)]
                .groupby('ts_code')
                .agg(mean_vol=('vol', 'mean'))
                .reset_index())
    vol_20_pct = (data[['ts_code','vol']][data['trade_date'] == trade_date]
                  .merge(
        mean_vol,
        how="left",
        on=['ts_code']
    ))
    vol_20_pct['vol_20_pct'] = vol_20_pct['vol'] / vol_20_pct['mean_vol']

    pretrade_date = get_pretrade_date(trade_date)
    first_double_vol = data[['ts_code','vol']][data['trade_date'] == trade_date].merge(
        data[['ts_code','vol']][data['trade_date'] == pretrade_date],
        how="left",
        on=['ts_code']
    ).merge(
        vol_20_pct,
        how="left",
        on=['ts_code']
    )
    first_double_vol['first_double_vol'] = (
        (first_double_vol['vol_x'] / first_double_vol['vol_y'] > 2)
        & (first_double_vol['vol_20_pct'] > 2.5)
    ).astype(int)

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
    ).merge(
        vol_20_pct[["ts_code", "vol_20_pct"]],
        how="left",
        on=["ts_code"]
    ).merge(
        first_double_vol[['ts_code', 'first_double_vol']],
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
            'vol_20_pct'
        ]
    ] = dm_stock_performance_data[
        [
            "up_5_next_open_pct_chg",
            "up_5_next_high_pct_chg",
            "up_5_next_close_pct_chg",
            "up_limit_next_open_pct_chg",
            "up_limit_next_high_pct_chg",
            "up_limit_next_close_pct_chg",
            'vol_20_pct'
        ]
    ].apply(lambda x: round(x, 4))

    # data_load.truncate("dm_stock_performance")
    data_load.append("dm_stock_performance", dm_stock_performance_data)







