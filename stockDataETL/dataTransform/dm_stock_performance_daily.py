from datetime import datetime
import pandas as pd
from pandas import DataFrame
from stockDataETL.dataLoad.DataLoad import DataLoad

def dm_stock_performance_daily(trade_date: str) -> str:

    data_load = DataLoad()
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
        where trade_date >= date_sub(:trade_date, interval 180 day)
        """,
        {
            "trade_date": trade_date
        }
    )
    data = DataFrame(data, columns=[
        "trade_date", "ts_code", "name", "industry","close","open_pct_chg",
        "open_pct_chg", "high_pct_chg","pct_chg","up_limit","down_limit",
    ])

    up_days = data[data['pct_chg'] > 0].groupby(["ts_code", "name", "industry"]).agg(
        up_days=("pct_chg", "count")
    )
    down_days = data[data['pct_chg'] <= 0].groupby(["ts_code", "name", "industry"]).agg(
        down_days=("pct_chg", "count")
    )
    up_5_days = data[data['pct_chg'] >= 0.05].groupby(["ts_code", "name", "industry"]).agg(
        up_5_days=("pct_chg", "count")
    )
    down_5_days = data[data['pct_chg'] <= -0.05].groupby(["ts_code", "name", "industry"]).agg(
        down_5_days=("pct_chg", "count")
    )
    up_limit_days = data[data['close'] == data['up_limit']].groupby(["ts_code", "name", "industry"]).agg(
        up_limit_days=("pct_chg", "count")
    )
    down_limit_days = data[data['close'] == data['down_limit']].groupby(["ts_code", "name", "industry"]).agg(
        down_limit_days=("pct_chg", "count")
    )
    up_5_data = data[data['pct_chg'] >= 0.05][["trade_date", "ts_code"]]
    up_5_data["trade_date"] = up_5_data["trade_date"].apply(lambda x:
                                                            datetime.strptime(x, "%Y-%m-%d")
                                                            .replace(day=datetime.strptime(x, "%Y-%m-%d").day + 1)
                                                            .strftime("%Y-%m-%d"))
    up_5_next_open_pct_chg = pd.merge(
        up_5_data,
        data[["trade_date", "ts_code", "open_pct_chg"]],
        how="left",
        on=["trade_date", "ts_code"]
    ).groupby(["ts_code"]).agg(
        up_5_next_open_pct_chg=("open_pct_chg", "mean")
    )


