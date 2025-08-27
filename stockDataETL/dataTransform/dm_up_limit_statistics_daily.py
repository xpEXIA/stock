from datetime import datetime, timedelta
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.commonUtils.get_pretrade_date import get_pretrade_date


def dm_up_limit_statistics_daily(trade_date: str) -> None:

    data_load = DataLoad()
    pretrade_date = get_pretrade_date(trade_date)
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
        where trade_date >= :pretrade_date and trade_date <= :trade_date
        """,
        {
            "pretrade_date": pretrade_date,
            "trade_date": trade_date
        }
    )
    data = DataFrame(data, columns=['trade_date', 'ts_code', 'close', 'open', 'high',
                                    'pre_close', 'open_pct_chg', 'up_limit'])
    data['trade_date'] = data['trade_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    dm_up_limit_statistics_data = {}
    dm_up_limit_statistics_data['trade_date'] = trade_date
    dm_up_limit_statistics_data['up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_date'] == trade_date)]['close'].count()
    dm_up_limit_statistics_data['pre_up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_date'] == pretrade_date)]['close'].count()
    ctu_up_limit_count_data = (data[(data['trade_date'] == trade_date) &
                                 (data['up_limit'] == data['close'])][['trade_date','ts_code']]
                             .merge(data[(data['trade_date'] == pretrade_date) &
                                 (data['up_limit'] == data['close'])][['trade_date','ts_code','close']]
                                    ,how='left', on='ts_code'))
    dm_up_limit_statistics_data['ctu_up_limit_count'] =  ctu_up_limit_count_data[ctu_up_limit_count_data['close'].notna()]['close'].count()
    if dm_up_limit_statistics_data['pre_up_limit_count'] != 0:
        dm_up_limit_statistics_data['ctu_up_limit_pct'] = round((dm_up_limit_statistics_data['ctu_up_limit_count']
                                                        / dm_up_limit_statistics_data['pre_up_limit_count']),4) * 100
    else:
        dm_up_limit_statistics_data['ctu_up_limit_pct'] = 0
    dm_up_limit_statistics_data['gradually_up_limit_count'] = data[(data['up_limit'] == data['close'])
                                                                   & (data['trade_date'] == trade_date)
                                                                   & (data['open'] < data['pre_close'] * 1.08)]['close'].count()
    dm_up_limit_statistics_data['open_pct_chg_more_than_5'] = data[(data['up_limit'] == data['close'])
                                                                   & (data['trade_date'] == trade_date)
                                                                   & (data['open_pct_chg'] > 5)]['close'].count()
    dm_up_limit_statistics_data['up_limit_open'] = data[(data['up_limit'] == data['high'])
                                                        & (data['trade_date'] == trade_date)
                                                        & (data['up_limit'] > data['close'])]['close'].count()
    dm_up_limit_statistics_data['open_pct_chg_lower'] = data[(data['up_limit'] == data['close'])
                                                         & (data['trade_date'] == trade_date)
                                                         & (data['open_pct_chg'] < 0)]['close'].count()

    dm_up_limit_statistics_data = DataFrame(dm_up_limit_statistics_data, index=[1])

    data_load.append('dm_up_limit_statistics', dm_up_limit_statistics_data)

