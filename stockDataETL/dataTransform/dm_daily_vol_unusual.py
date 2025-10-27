from datetime import datetime

import pandas as pd
from pandas import DataFrame
from stockDataETL import logger
from stockDataETL.dataLoad.DataLoad import DataLoad
from stockDataETL.dataTransform.commonUtils.get_pretrade_date import get_pretrade_date


def dm_daily_vol_unusual(trade_date: str) -> None:

    data_load = DataLoad()
    logger.info(f"开始处理股票日交易量异常数据, 交易日:{trade_date}, 表dm_daily_vol_unusual")

    logger.info("获取数据计算起始日期")
    cal_data = data_load.search(
        """select cal_date, is_open
           from ods_trade_cal
           where cal_date <= :end_date
             and id <= (
               select id + 50
               from ods_trade_cal
               where cal_date = :end_date
               )
            and is_open = 1
        """,
        {
            "end_date": datetime.strptime(trade_date, "%Y-%m-%d").strftime("%Y%m%d")
        }
    )
    cal_data = DataFrame(cal_data, columns=["cal_date", "is_open"])
    start_date = cal_data.iloc[30]["cal_date"]
    pretrade_date = datetime.strptime(get_pretrade_date(trade_date), "%Y-%m-%d").strftime("%Y%m%d")

    trade_data = data_load.search(
        """select ts_code,name,market,trade_date,close,pct_chg,vol,turnover_rate_f,
                pe,circ_mv,buy_elg_net_amount + buy_lg_net_amount as big_money_net_amount,
                net_d5_amount,ctu_up_days,ctu_down_days
           from dw_daily_trends
           where trade_date >= :start_date
             and trade_date <= :end_date
        """,
        {
            "end_date": trade_date,
            "start_date": start_date
        }
    )
    trade_data = DataFrame(
        trade_data,
        columns=[
            "ts_code", "name", "market", "trade_date",
            "close", "pct_chg", "vol", "turnover_rate_f",
            "pe", "circ_mv", "big_money_net_amount",
            "net_d5_amount", "ctu_up_days", "ctu_down_days"
        ]
    )
    trade_data['trade_date'] = trade_data['trade_date'].apply(lambda x: x.strftime("%Y%m%d"))

    pre_data = (trade_data[(trade_data['trade_date'] < pretrade_date)
                          & (trade_data['trade_date'] >= start_date)]
                .groupby('ts_code')
                .agg(
                    {'vol': 'mean',
                     'close': 'median'}
                ).reset_index().rename(columns={'vol': 'vol_mean', 'close': 'close_median'}))

    trade_date = datetime.strptime(trade_date, "%Y-%m-%d").strftime("%Y%m%d")
    vol_data = (trade_data[trade_data['trade_date'] == trade_date][
                    ['ts_code', 'name', 'trade_date', 'close',
                     'pct_chg', 'market', 'vol', 'turnover_rate_f',
                     "pe", "circ_mv", "big_money_net_amount",
                     "net_d5_amount", "ctu_up_days", "ctu_down_days"]
                ].merge(pre_data, on='ts_code', how='left'))

    vol_data['vol_pct'] = round(vol_data['vol'] / vol_data['vol_mean'], 2)
    vol_data['close_pct'] = round((vol_data['close'] - vol_data['close_median']) / vol_data['close_median'], 4) * 100
    vol_data.sort_values(by=['close_pct', 'vol_pct'], ascending=False, inplace=True)
    last_data = trade_data[trade_data['trade_date'] == pretrade_date][['ts_code', 'vol']].rename(columns={'vol': 'last_vol'})
    vol_data = vol_data.merge(last_data, on=['ts_code'], how='left')
    vol_data['vol_pct_last'] = round(vol_data['vol'] / vol_data['last_vol'], 2)
    dm_daily_vol_unusual_data = vol_data[(vol_data['vol_pct'] > 2.5)
                    & (~vol_data['market'].isin(['科创板', '北交所']))
                    & (vol_data['turnover_rate_f'] >= 15)
                    & (vol_data['vol_pct_last'] <= 2)]

    dm_daily_vol_unusual_data = dm_daily_vol_unusual_data[['ts_code', 'name', 'trade_date', 'market', 'vol', 'vol_pct',
                                                          'vol_pct_last', 'close', 'close_pct', 'pct_chg', 'pe',
                                                          'circ_mv', 'big_money_net_amount', 'net_d5_amount',
                                                          'ctu_up_days', 'ctu_down_days']]
    data_load.append("dm_daily_vol_unusual", dm_daily_vol_unusual_data)