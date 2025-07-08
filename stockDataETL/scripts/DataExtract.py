
from pandas.core.interchange.dataframe_protocol import DataFrame
from stockDataETL import ts_api

class GetTSData:

    def __init__(self, ts_api):

        self.ts_api = ts_api

    def getStockBasic(self, ts_code: str = "", list_status: str = "L", exchange: str = "SSE,SZSE") -> DataFrame:

        return self.ts_api.stock_basic(stock_code=ts_code, list_status=list_status, exchange=exchange)

    def getTradeCal(self, exchange: str = "SSE", start_date: str = "", end_date: str = "") -> DataFrame:

        return self.ts_api.trade_cal(exchange=exchange,start_date=start_date,end_date=end_date)

    def getDaily(self, ts_code: str = "", trade_date: str = "", start_date: str = "", end_date: object = "") -> DataFrame:

        return self.ts_api.daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)

    def getStockCompany(self, ts_code: str = "", exchange: str = "") -> DataFrame:

        return self.ts_api.stock_company(ts_code=ts_code,exchange=exchange)

