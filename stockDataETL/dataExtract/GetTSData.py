from pandas.core.interchange.dataframe_protocol import DataFrame
from stockDataETL import ts_api
from stockDataETL import logging

class GetTSData:
    """
    获取TuShare股票数据
    """

    def __init__(self, ts_api=ts_api):

        self.ts_api = ts_api

    def getStockBasic(self, ts_code: str = "", list_status: str = "L", exchange: str = "") -> DataFrame:

        # 获取股票基础信息
        try:
            logging.info("request stock basic data")
            return self.ts_api.stock_basic(stock_code=ts_code, list_status=list_status, exchange=exchange)
        except Exception as e:
            logging.error("request stock basic data failure", exc_info=True)
            return None

    def getTradeCal(self, exchange: str = "SSE", start_date: str = "", end_date: str = "") -> DataFrame:

        # 获取交易日历数据
        try:
            logging.info("request trade cal data")
            return self.ts_api.trade_cal(exchange=exchange,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request trade cal data failure", exc_info=True)
            return None

    def getDaily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日行情数据
        try:
            logging.info("request daily data")
            return self.ts_api.daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request daily data failure", exc_info=True)
            return None

    def getStockCompany(self, ts_code: str = "", exchange: str = "") -> DataFrame:

        # 获取股票公司信息
        try:
            logging.info("request stock company data")
            return self.ts_api.stock_company(ts_code=ts_code,exchange=exchange)
        except Exception as e:
            logging.error("request stock company data failure", exc_info=True)
            return None

    def getDailyBasic(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日指标数据
        try:
            logging.info("request daily basic data")
            return self.ts_api.daily_basic(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request daily basic data failure", exc_info=True)
            return None

    def getIndexBasic(self, ts_code: str = "", name: str = "", market: str = "", publisher: str = "", category: str = "" ) -> DataFrame:

        # 获取指数基本信息
        try:
            logging.info("request index basic data")
            return self.ts_api.index_basic(ts_code=ts_code,name=name,market=market,publisher=publisher,category=category)
        except Exception as e:
            logging.error("request index basic data failure", exc_info=True)
            return None

    def getIndexDaily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取指数日线数据
        try:
            logging.info("request index daily data")
            return self.ts_api.index_daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request index daily data failure", exc_info=True)
            return None

    def getMoneyFlow(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取资金流向
        try:
            logging.info("request money flow data")
            return self.ts_api.moneyflow(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request money flow data failure", exc_info=True)
            return None

    def getStkLimit(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        # 获取每日涨跌停价
        try:
            logging.info("request stk limit data")
            return self.ts_api.stk_limit(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)
        except Exception as e:
            logging.error("request stk limit data failure", exc_info=True)
            return None




