from datetime import datetime
from stockDataETL.dataLoad.DataLoad import DataLoad


def get_pretrade_date(trade_date: str) -> str:

    data_load = DataLoad()
    pretrade_date = data_load.search(
        """
        select pretrade_date
        from ods_trade_cal
        where cal_date = :trade_date
        """,
        {
            "trade_date": datetime.strptime(trade_date, "%Y-%m-%d").strftime("%Y%m%d")
            # "trade_date": "20250716"
        }
    )[0][0]
    data_load.close()
    return pretrade_date