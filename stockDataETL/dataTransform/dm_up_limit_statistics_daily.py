from datetime import datetime, timedelta
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad


def dm_up_limit_statistics_daily(trade_date: str) -> None:

    data_load = DataLoad()
    pre_trade_date =((datetime.strptime(trade_date, "%Y-%m-%d")
                           - timedelta(days=1))
                     .strftime("%Y-%m-%d"))
    logger.info(f"开始处理股票涨停数据, 交易日:{trade_date}, 表dm_up_limit_statistics")
    data  = data_load.search(
        """
        select 
            trade_date,
            ts_code,
            close,
            open,
            high,
            pre_close,
            open_pct_chg,
            up_limit
        from dw_daily_trends
        where trade_date >= :trade_date
        """,
        {
            "trade_date": pre_trade_date
        }
    )
    data = DataFrame(data, columns=['trade_code', 'ts_code', 'close', 'open', 'high',
                                    'pre_close', 'open_pct_chg', 'up_limit'])

    dm_up_limit_statistics_data = {}
    dm_up_limit_statistics_data['trade_date'] = trade_date
    dm_up_limit_statistics_data['up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_code'] == trade_date)].count()
    dm_up_limit_statistics_data['pre_up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_code'] == pre_trade_date)].count()
    dm_up_limit_statistics_data['ctu_up_limit_count'] = (data[(data['trade_data'] == trade_date) &
                                                             (data['up_limit'] == data['close'])][['trade_data','ts_code']]
                                                         .merge(data[(data['trade_data'] == pre_trade_date) &
                                                             (data['up_limit'] == data['close'])][['trade_data','ts_code']]
                                                                ,how='left', on='ts_code'))['y_ts_code'].count()
    dm_up_limit_statistics_data['ctu_up_limit_pct'] = (dm_up_limit_statistics_data['ctu_up_limit_count']
                                                       / dm_up_limit_statistics_data['pre_up_limit_count'])
    dm_up_limit_statistics_data['gradually_up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                                   & (data['trade_code'] == trade_date)
                                                                   & (data['open'] < data['pre_close'] * 1.08)]
    dm_up_limit_statistics_data['open_pct_chg_more_than_5'] = data[(data['up_limit'] == data['close'])
                                                                   & (data['trade_code'] == trade_date)
                                                                   & (data['open'] > 0.05)]
    dm_up_limit_statistics_data['up_limit_open'] = data[(data['up_limit'] == data['high'])
                                                        & (data['trade_code'] == trade_date)
                                                        & (data['up_limit'] > data['close'])]
    dm_up_limit_statistics_data['open_pct_chg_lower'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_code'] == trade_date)
                                                         & (data['open'] < 0)]

    dm_up_limit_statistics_data = DataFrame(dm_up_limit_statistics_data, index=[0])

    data_load.append('dm_up_limit_statistics', dm_up_limit_statistics_data)
    data_load.close()