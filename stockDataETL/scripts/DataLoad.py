
from django.db import connection
from pandas import DataFrame
from stockDataETL import logging

class DataLoad:

    def __init__(self):

        self.stock_connect = connection['stock'].cursor.db.connection

    def getStockConnect(self):

        logging.info("Get stock connect")
        return self.stock_connect

    def truncate(self, table_name: str):

        cursor = self.getStockConnect()
        cursor.execute("TRUNCATE TABLE" + table_name + ";")
        logging.warn("Truncate table " + table_name)

    def append(self, table_name: str, data: DataFrame):

        cursor = self.getStockConnect()
        data.to_sql(table_name, cursor, if_exists='append', index=False)
        logging.info("Append data to " + table_name + ", successfully append " + len(data) + " rows")

    def closeConnect(self):

        logging.info("Close stock connect")
        self.stock_connect.close()




