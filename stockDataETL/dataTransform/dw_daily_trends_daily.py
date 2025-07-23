import os
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad


def dw_daily_trends_daily(trade_date: str,
                          file_path: str = "/scripts/dw_daily_trends_daily.sql"):

    data_load = DataLoad()
    logger.info("开始处理股票日趋势数据, 表dw_daily_trends")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL文件不存在: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
            data_load.execute(sql_content,
                              "dw_daily_trends_daily",
                              {'trade_date': trade_date})
        logger.info("dw_daily_trends_daily执行成功")
    except Exception as e:
        raise Exception(f"执行SQL文件失败: {str(e)}")

    data_load.close()