from stockDataETL.dataLoad.DataLoad import DataLoad


def trade_date_complete_check(table: str,
                        connect: object = DataLoad()) -> str:

    data_load = connect
    data_load.execute(
        """
        select
            count(trade_date)
        from (
                select
                str_to_date(cal_date, '%Y%m%d') as cal_date
                from ods_trade_cal
                where is_open = '1' and cal_date >= '2024-07-30' and cal_date <= '2025-07-28'
            ) cal_date left join (
                 select
                     trade_date
                 from :table
            ) check_table on check_table.trade_date = cal_date.cal_date
        where check_table.trade_date is null
        """,
        {
            "table": table,
        }
    )