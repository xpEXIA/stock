from operator import index

from pandas.core.interchange.dataframe_protocol import DataFrame

from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad


def trade_date_complete_check(table: str,
                              start_date: str,
                              end_date: str,
                              connect: object = DataLoad()) -> str:

    data_load = connect
    fail_date = data_load.search(
        """
        select
            cal_date.cal_date
        from (
                select
                str_to_date(cal_date, '%Y%m%d') as cal_date
                from ods_trade_cal
                where is_open = '1' and cal_date >= :start_date and cal_date <= :end_date
            ) cal_date left join (
                 select
                     trade_date
                 from :table
            ) check_table on check_table.trade_date = cal_date.cal_date
        where check_table.trade_date is null
        """,
        {
            "table": table,
            "start_date": start_date,
            "end_date": end_date
        }
    )

    logger.info(f"数据获取失败的交易日为: {fail_date}")