
from pandas.core.interchange.dataframe_protocol import DataFrame
from stockDataETL import ts_api
from stockDataETL import logging

class GetTSData:
    """
    获取TuShare股票数据
    """

    def __init__(self, ts_api=ts_api):

        self.ts_api = ts_api

    def getStockBasic(self, ts_code: str = "", list_status: str = "L", exchange: str = "SSE,SZSE") -> DataFrame:

        # 获取股票基础信息
        logging.info("request stock basic data")
        return self.ts_api.stock_basic(stock_code=ts_code, list_status=list_status, exchange=exchange)

    def getTradeCal(self, exchange: str = "SSE", start_date: str = "", end_date: str = "") -> DataFrame:

        # 获取交易日历数据
        logging.info("request trade cal data")
        return self.ts_api.trade_cal(exchange=exchange,start_date=start_date,end_date=end_date)

    def getDaily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日行情数据
        logging.info("request daily data")
        return self.ts_api.daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

    def getStockCompany(self, ts_code: str = "", exchange: str = "") -> DataFrame:

        # 获取股票公司信息
        logging.info("request stock company data")
        return self.ts_api.stock_company(ts_code=ts_code,exchange=exchange)

    def getDailyBasic(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日指标数据
        logging.info("request daily basic data")
        return self.ts_api.daily_basic(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

    def getIndexBasic(self, ts_code: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取指数基本信息
        logging.info("request index basic data")
        return self.ts_api.index_basic(ts_code=ts_code,start_date=start_date,end_date=end_date)

    def getIndexDaily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日指标数据
        logging.info("request index daily data")
        return self.ts_api.index_daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

    def getMoneyFlow(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取资金流向
        logging.info("request money flow data")
        return self.ts_api.moneyflow(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

    def getStkLimit(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日涨跌停价
        logging.info("request stk limit data")
        return self.ts_api.stk_limit(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)




