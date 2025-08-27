from contextlib import contextmanager
from pandas import DataFrame
from stockDataETL import logging, engine
from sqlalchemy import text


class DataLoad:

    def __init__(self):

        self.engine = engine  # 保存 engine 而不是连接

    def get_new_connection(self):
        return self.engine.connect()

    def truncate(self, table_name: str):

        conn = None
        try:
            conn = self.get_new_connection()
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            conn.commit()
            logging.warn(f"Truncate table {table_name}")
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Truncate table {table_name} failed", exc_info=True)
        finally:
            if conn:
                conn.close()

    def append(self, table_name: str, data: DataFrame):

        conn = None
        try:
            conn = self.get_new_connection()
            data.to_sql(table_name, con=conn, if_exists='append', index=False)
            conn.commit()
            logging.info(f"Append data to {table_name}, successfully append {len(data)} rows")
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error(f"Append data to {table_name} failed", exc_info=True)
        finally:
            if conn:
                conn.close()

    def search(self, sql: str, parameters: dict = None):

        try:
            with self.get_new_connection() as conn:
                result = conn.execute(text(sql), parameters).fetchall()
                logging.info("Search '" + sql + "' successfully")
                return result
        except Exception as e:
            logging.error("Search '" + sql + "' failed", exc_info=True)
            return None

    def execute(self, sql: str, sql_name: str, parameters: dict = None):

        conn = None
        try:
            conn = self.get_new_connection()
            result = conn.execute(text(sql), parameters)
            conn.commit()
            logging.info("Execute '" + sql_name + "' successfully")
            return result
        except Exception as e:
            if conn:
                conn.rollback()
            logging.error("Execute '" + sql + "' failed", exc_info=True)
            return None
        finally:
            if conn:
                conn.close()


    def close(self):

        self.get_new_connection.close()
        logging.info("Database connection closed")




