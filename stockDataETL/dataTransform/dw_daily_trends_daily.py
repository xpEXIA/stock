import os
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad


def dw_daily_trends_daily(
        trade_date: str,
        file_path: str = "./stockDataETL/dataTransform/scripts/dw_daily_trends_daily.sql",
) -> None:


    data_load = DataLoad()
    logger.info(f"开始处理股票日趋势数据, 交易日:{trade_date}, 表dw_daily_trends")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL文件不存在: {file_path}")

    file = open(file_path, 'r', encoding='utf-8')
    sql_content = file.read().replace(':trade_date', trade_date)
    data_load.execute(
        sql_content,
        "dw_daily_trends_daily")
    logger.info(f"dw_daily_trends_daily执行成功, 交易日:{trade_date}")


