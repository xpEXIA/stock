from symtable import Class

from stockDataAcquisition import ts_api

class DataAcquisition:

    def __init__(self, stock_code):

        self.ts_api = ts_api()

    def getStockBasic(self,ts_code="",list_status="L",exchange="SSE,SZSE"):

        return self.ts_api.stock_basic(stock_code=ts_code, list_status=list_status, exchange=exchange)

    def getTradeCal(self,exchange="SSE",start_date="",end_date=""):

        return self.ts_api.trade_cal(exchange=exchange,start_date=start_date,end_date=end_date)

    def getDaily(self,ts_code="",trade_date="",start_date="",end_date=""):

        return self.ts_api.daily(ts_code=ts_code,trade_date=trade_date,start_date=start_date,end_date=end_date)