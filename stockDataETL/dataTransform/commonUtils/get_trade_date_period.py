from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from stockDataETL import logger, engine


def get_trade_date_period(
        date: str,
        period: int,
        mode: str = 'pre'
) -> list:
    """
    获取指定时间段内的交易日
    :param date: 计算日期，格式为"%Y-%m-%d"
    :param period: 交易日周期，int
    :param mode: 获取模式，'pre'为向前获取，‘post’
    :return: 交易日列表
    """

    if mode=='pre':
        cal_date = pd.read_sql(
            text(f"""
            select cal_date,
                   is_open
            from ods_trade_cal
            where cal_date <= :trade_date
              and cal_date >= :trade_date_st
            order by cal_date desc
            """),
            params={
                "trade_date": datetime.strptime(date, "%Y-%m-%d").strftime('%Y%m%d'),
                "trade_date_st": (
                        datetime.strptime(date, "%Y-%m-%d") - timedelta(days=20+period)
                ).strftime('%Y%m%d')
            },
            con=engine
        )

        n = 0
        for i in cal_date.cal_date.tolist():
            n += int(cal_date[cal_date.cal_date == i]['is_open'].iloc[0])
            if n == 2:
                end_date = datetime.strptime(i, "%Y%m%d").strftime("%Y-%m-%d")
            elif n == 1 + period:
                start_date = datetime.strptime(i, "%Y%m%d").strftime("%Y-%m-%d")
    else:
        cal_date = pd.read_sql(
            text(f"""
            select cal_date,
                   is_open
            from ods_trade_cal
            where cal_date >= :trade_date
              and cal_date <= :trade_date_end
            order by cal_date asc
            """),
            params={
                "trade_date": datetime.strptime(date, "%Y-%m-%d").strftime('%Y%m%d'),
                "trade_date_st": (
                        datetime.strptime(date, "%Y-%m-%d") - timedelta(days=20+period)
                ).strftime('%Y%m%d')
            },
            con=engine
        )

        n = 0
        for i in cal_date.cal_date.tolist():
            n += int(cal_date[cal_date.cal_date == i]['is_open'].iloc[0])
            if n == 2:
                start_date = datetime.strptime(i, "%Y%m%d").strftime("%Y-%m-%d")
            elif n == 1 + period:
                end_date = datetime.strptime(i, "%Y%m%d").strftime("%Y-%m-%d")
    logger.info(f"获取量比计算周期成功, 时间段为{start_date}至{end_date}")
    return [start_date, end_date]