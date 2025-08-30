from datetime import datetime

from pandas import DataFrame

from stockDataETL.dataLoad.DataLoad import DataLoad


def get_pretrade_date(
        trade_date: str,
        interval: int = 0
) -> str:

    """
    获取指定交易日的前一个交易日
    :param trade_date: 交易日，仅支持"%Y-%m-%d"
    :return: 前一个交易日
    """

    data_load = DataLoad()
    pretrade_date = data_load.search(
        """
        select 
            cal_date,
            pretrade_date
        from ods_trade_cal
        where cal_date >= date_sub(:trade_date, interval :interval day) and cal_date <= :trade_date
        """,
        {
            "trade_date": datetime.strptime(trade_date, "%Y-%m-%d").strftime("%Y%m%d"),
            "interval": interval
            # "trade_date": "20250716"
        }
    )
    pretrade_date = DataFrame(pretrade_date, columns=[
        "cal_date", "pretrade_date"
    ])

    if interval == 0:
        return datetime.strptime(pretrade_date['pretrade_date'].values[0], "%Y%m%d").strftime("%Y-%m-%d")
    else:
        return pretrade_date