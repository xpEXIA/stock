from datetime import datetime
import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from stockDataETL import logger, engine
from stockDataETL.dataExtract.GetMyData import GetMyData
from stockDataETL.dataTransform.commonUtils.get_trade_date_period import get_trade_date_period


def dm_daily_one_night_stock_history(start_time: str, end_time: str, period: int = 5) -> None:
    """
    获取一夜持股历史数据
    :param ts_code: 股票代码
    :param start_time:开始时间，例如：'20240101' 或'20241231235959'
    :param end_time:结束时间，例如：'20240101' 或'20241231235959'
    :param period: 历史成交量计算周期，默认为过去5个交易日
    :return:None，完成数据库写入
    """
    # 获取股票列表
    stock_list = pd.read_sql(
        text("""
            select
                ts_code
            from stock.ods_stock_basic
            where market in ('主板','创业板')
        """),
        con=engine
    )
    logger.info(f"获取股票列表成功")

    # 获取股票历史14:30股价数据
    getMydata = GetMyData()
    cal_data_list = []
    n = 0
    for ts_code in stock_list.ts_code.tolist():
        history_data = getMydata.getMyHistoryTime(ts_code, start_time, end_time)
        if history_data.empty:
            continue
        history_data['ts_code'] = ts_code
        history_data['time'] = history_data.trade_time.apply(lambda x: x[11:19])
        history_data['trade_date'] = history_data.trade_time.apply(lambda x: x[:10])
        cal_data_list.append(history_data[['ts_code', 'trade_date', 'trade_time', 'close']][history_data.time == '14:30:00'])
        n += 1
    cal_data = pd.concat(cal_data_list,ignore_index=True)
    logger.info(f"获取股票历史数据成功，共完成{n}只股票获取")

    # 获取股票历史其他数据，补充14:30股价数据
    dw_data = pd.read_sql(
        text(f"""
        select
            trade_date,
            ts_code,
            name,
            market,
            open,
            high,
            low,
            pre_close,
            amout,
            vol,
            turnover_rate_f,
            pe,
            net_d5_amount
        from dw_daily_trends
        where trade_date >= :start_date and trade_date <= :end_date
        """),
        params={
            'start_date': datetime.strptime(start_time[:8], "%Y%m%d").strftime("%Y-%m-%d"),
            'end_date': datetime.strptime(end_time[:8], "%Y%m%d").strftime("%Y-%m-%d")
        },
        con=engine
    )
    dw_data['trade_date'] = dw_data['trade_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    logger.info(f"获取dw_daily_trends股票历史数据成功")

    cal_data = cal_data.merge(dw_data, how='left', on=['ts_code', 'trade_date'])
    cal_data['close'] = pd.to_numeric(cal_data['close'], errors='coerce')
    cal_data.dropna(axis=0,how='any',subset=['close','pre_close'], inplace=True)
    cal_data['pct_chg'] = round((cal_data.close - cal_data.pre_close) / cal_data.pre_close * 100, 2)

    avg_data_list = []
    # 获取过去5个交易日各股票数据，计算量比
    for trade_date in cal_data['trade_date'].drop_duplicates().tolist():
        start_date, end_date = get_trade_date_period(trade_date, period)
        data = pd.read_sql(
            text(f"""
            select
                ts_code,
                ROUND(avg(circ_mv),2) avg_circ_mv,
                ROUND(avg(vol),2) avg_vol
            from dw_daily_trends
            where trade_date >= :start_date and trade_date <= :end_date
            group by ts_code
            """),
            params={
                'start_date': start_date,
                'end_date': end_date
            },
            con=engine
        )
        data['trade_date'] = trade_date
        avg_data_list.append(data)
    avg_data = pd.concat(avg_data_list, ignore_index=True)
    logger.info(f"获取过去5个交易日各股票数据成功")

    cal_data = cal_data.merge(avg_data, how='left', on=['ts_code', 'trade_date'])
    cal_data['vol_ratio'] = round(cal_data.vol / cal_data.avg_vol, 2)
    result = cal_data[
        (cal_data.pct_chg >= 3) & (cal_data.pct_chg <= 6) &
        (cal_data.avg_circ_mv >= 500000) & (cal_data.avg_circ_mv <= 2200000) &
        (cal_data.turnover_rate_f >= 5) & (cal_data.turnover_rate_f <= 15) &
        (cal_data.vol_ratio > 1) &
        ~cal_data.name.str.contains('ST', na=False)
    ]
    result.drop(columns=['trade_date'], inplace=True)
    result.rename(columns={
        'trade_time': 'update_time',
        'close': 'realtime',
    }, inplace=True)
    logger.info(f"完成一日持股历史股票数据筛选")
    result.to_sql('dm_daily_one_night_stock', con=engine, if_exists='append', index=False)
    logger.info(f"完成一日持股历史股票数据写入")

