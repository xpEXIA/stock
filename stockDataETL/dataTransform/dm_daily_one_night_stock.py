from datetime import datetime, timedelta
import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.commonUtils.get_trade_date_period import get_trade_date_period
from stockDataETL.manualAnalysis.database_conn import engine
from stockDataETL.dataExtract.GetMyData import GetMyData


def dm_daily_one_night_stock(trade_date: str, period: int = 5) -> None:
    """
    计算当日一夜情股票数据
    :param trade_date: 交易日，格式如'2025-12-05'
    :param period:
    """
    # 将trade_date转换为datetime对象
    cal_date = pd.read_sql(
        """
        select
            cal_date,
            is_open
        from ods_trade_cal
        where cal_date = :trade_date 
        """,
        {
            "trade_date": trade_date,
        }
    )

    if cal_date['is_open'].iloc[0] == 0:
        logger.info(f"{trade_date}非交易日")
        raise Exception(f"{trade_date}非交易日")

    # 计算5日前的日期
    start_date, end_date = get_trade_date_period(trade_date, period)
    
    # 获取股票列表
    stock_list = pd.read_sql(
        text("""
            select
                ts_code
            from stock.ods_stock_basic
        """),
        con=engine
    )
    logger.info(f"获取股票列表成功")
    
    # 计算5日平均成交量和资金净流入
    avg_vol_5 = pd.read_sql(
        text(f"""
        select
            ts_code,
            name,
            market,
            ROUND(avg(circ_mv),2) avg_circ_mv,
            ROUND(avg(vol),2) avg_vol,
            ROUND(sum(net_mf_amount),2) net_d5_amount
        from dw_daily_trends
        where trade_date >= :start_date and trade_date <= :end_date
        group by ts_code,name,market
        """),
        params={
            'start_date': start_date,
            'end_date': end_date
        },
        con=engine
    )
    logger.info(f"获取5日平均成交量和资金净流入数据成功")
    
    # 获取实时数据
    getMydata = GetMyData()
    cal_data = getMydata.getRealTimeDaily(ts_code_list=stock_list.ts_code.tolist())
    cal_data = cal_data.merge(avg_vol_5, how='left', on=['ts_code'])
    
    # 计算成交量比率
    cal_time = cal_data['update'].iloc[0]
    cal_time = datetime.strptime(cal_time, "%Y-%m-%d %H:%M:%S")
    if cal_time.hour < 13:
        cal_time_num = cal_time - datetime(cal_time.year, cal_time.month, cal_time.day, 9, 30, 0)
        cal_time_num = cal_time_num.seconds / 60
    else:
        cal_time_num = cal_time - datetime(cal_time.year, cal_time.month, cal_time.day, 13, 0, 0)
        cal_time_num = cal_time_num.seconds / 60 + 120
    cal_data['vol_ratio'] = round((cal_data.vol / cal_time_num) * 240 / cal_data.avg_vol, 2)
    
    # 根据条件筛选股票
    result = cal_data[
        (cal_data.pct_chg >= 3) & (cal_data.pct_chg <= 6) &
        (cal_data.avg_circ_mv >= 500000) & (cal_data.avg_circ_mv <= 2200000) &
        (cal_data.turnover_rate_f >= 5) & (cal_data.turnover_rate_f <= 15) &
        (cal_data.vol_ratio > 1) &
        ~cal_data.name.str.contains('ST', na=False)
    ]
    logger.info(f"筛选出{len(result)}只符合条件的股票")
    result = result[[
        'update',
        'ts_code',
        'name',
        'market',
        'realtime',
        'open',
        'high',
        'low',
        'pre_close',
        'amout',
        'vol',
        'pct_chg',
        'pe',
        'turnover_rate_f',
        'vol_ratio',
        'avg_circ_mv',
        'avg_vol',
        'net_d5_amount'
    ]].rename(columns={
        'update': 'update_time',
    })

    result.to_sql('dm_daily_one_night_stock',con=engine,if_exists='append',index=False)
    
