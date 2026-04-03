from datetime import datetime, timedelta
from pandas import DataFrame
from stockDataETL.dataLoad.DataLoad import DataLoad


def get_pretrade_date(
        start_date: str,
        end_date: str
) -> list:

    """
    获取指定周期内的所有交易日
    :param start_date: 开始日期，仅支持"%Y-%m-%d"
    :param end_date: 结束日期，仅支持"%Y-%m-%d"
    :return: 所有交易日
    """

    data_load = DataLoad()
    pretrade_date = data_load.search(
        """
        select 
            cal_date,
            is_open
        from ods_trade_cal
        where cal_date >= :start_date 
            and cal_date <= :end_date
            and is_open = 1
        """,
        {
            "start_date": datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d"),
            'end_date': datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
        }
    )
    pretrade_date = DataFrame(pretrade_date, columns=[
        "cal_date", "is_open"
    ])

    cal_date_list = pretrade_date["cal_date"].tolist()

    return cal_date_list