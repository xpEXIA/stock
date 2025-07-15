
from pandas import DataFrame
from stockDataETL import logging, engine
from sqlalchemy import text


class DataLoad:

    def __init__(self):

        self.stock_connect = engine.connect()

    def getStockConnect(self):

        logging.info("Get stock connect")
        return self.stock_connect

    def truncate(self, table_name: str):

        try:
            self.conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            self.conn.commit()
            logging.warn(f"Truncate table {table_name}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Truncate table {table_name} failed", exc_info=True)

    def append(self, table_name: str, data: DataFrame):

        try:
            data.to_sql(table_name, con=self.engine, if_exists='append', index=False)
            logging.info(f"Append data to {table_name}, successfully append {len(data)} rows")
        except Exception as e:
            logging.error(f"Append data to {table_name} failed", exc_info=True)

    def search(self, sql: str):

        try:
            result = self.conn.execute(text(sql)).fetchall()
            logging.info("Search '" + sql + "' successfully")
            return result
        except Exception as e:
            logging.error("Search '" + sql + "' failed", exc_info=True)
            return None

    def close(self):

        self.conn.close()
        logging.info("Database connection closed")




