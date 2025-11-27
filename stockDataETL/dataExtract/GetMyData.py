import requests
from django.db import connection
from pandas.core.interchange.dataframe_protocol import DataFrame
from pandas import DataFrame
from stockDataETL import logger


class GetMyData:
    """
    获取MyData股票数据
    MyData官网：https://mairuiapi.com/hsdata
    """

    def __init__(self):

        self.license_key = "AF98235C-E4B0-4179-9C17-10955CAFD60B"

    def getMyRealTimeDaily(self, ts_code_list: list = []) -> DataFrame:

        """
        获取股票实时日线数据，
        API接口：http://api.mairuiapi.com/hsrl/ssjy_more/您的licence?stock_codes=股票代码1,股票代码2……股票代码20
        演示URL：http://api.mairuiapi.com/hsrl/ssjy_more/LICENCE-66D8-9F96-0C7F0FBCD073?stock_codes=000001,000002,000004
        接口说明：一次性获取《股票列表》中不超过20支股票的实时交易数据（您可以理解为日线的最新数据）
        数据更新：实时
        请求频率：1分钟300次 | 包年版1分钟3千次 | 钻石版1分钟6千次
        :return:
        """

        try:
            logger.info("request real time daily data")
            if len(ts_code_list) > 20:
                logger.error("request real time daily data failure, ts_code_list length > 20")
                return None
            ts_code = ",".join(ts_code_list)
            url = ("http://api.mairuiapi.com/hsrl/ssjy_more/"
                   + self.license_key
                   + "?stock_codes="
                   + ts_code)
            response = requests.get(url)
            if response.status_code != 200:
                logger.error(f"request real time daily data failed with status code: {response.status_code}")
                return None
            if not response.text.strip():
                logger.error("real time daily API returned empty response")
                return None
            """
            数据格式
            字段名称	数据类型	字段说明
            p	number	最新价
            o	number	开盘价
            h	number	最高价
            l	number	最低价
            yc	number	前收盘价
            cje	number	成交总额
            v	number	成交总量
            pv	number	原始成交总量
            ud	float	涨跌额
            pc	float	涨跌幅
            zf	float	振幅
            t	string	更新时间
            pe	number	市盈率
            tr	number	换手率
            pb_ratio	number	市净率
            tv	number	成交量
            """
            result = (
                DataFrame(response.json())
                .rename(columns={"p": "realtime",
                                "o": "open",
                                "h": "high",
                                "l": "low",
                                "yc": "pre_close",
                                "cje": "amout",
                                "v": "vol",
                                "pv": "p_vol",
                                "ud": "change",
                                "pc": "pct_chg",
                                "zf": "zf",
                                "t":"update",
                                "pe":"pe",
                                "tr":"turnover_ratio_f",
                                "pd_ratio":"pd_ratio",
                                "tv":"t_vol",})
            )
            return result
        except Exception as e:
            logger.error("request real time daily data failure", exc_info=True)
            return None

    def getStockList(self) -> list:

        """
        从数据库获取全部股票的ts_code列表
        从ods_stock_basic表中查询所有上市状态的股票代码
        :return: ts_code列表
        """
        try:
            logger.info("开始从数据库获取股票列表")
            # 使用Django数据库连接执行查询
            with connection.cursor() as cursor:
                # 查询ods_stock_basic表中的所有ts_code
                query = "SELECT ts_code FROM ods_stock_basic ORDER BY ts_code"
                cursor.execute(query)
                # 获取所有结果
                results = cursor.fetchall()
            
            # 提取ts_code列并转换为列表
            ts_code_list = [row[0] for row in results]
            logger.info(f"成功获取股票列表，共{len(ts_code_list)}支股票")
            return ts_code_list
        except Exception as e:
            logger.error("获取股票列表失败", exc_info=True)
            return []

    def getRealTimeDaily(self, limit_count: int = None, ts_code_list: list = []) -> DataFrame:
        """
        获取所有股票的实时日线数据
        通过调用getStockList获取所有股票代码，然后分批处理（每批20支）
        
        :param limit_count: 限制获取的股票数量，用于测试，默认获取所有
        :return: 包含所有股票实时数据的DataFrame
        """
        try:
            logger.info("开始获取所有股票实时数据")
            
            # 获取所有股票代码列表
            if not ts_code_list:
                ts_code_list = self.getStockList()
                if not ts_code_list:
                    logger.error("未能获取股票代码列表")
                    return None
            
            # 如果设置了限制数量，则只获取指定数量的股票
            if limit_count is not None and limit_count > 0:
                ts_code_list = ts_code_list[:limit_count]
                logger.info(f"已限制获取前{limit_count}支股票的实时数据")
            
            # 重要：移除股票代码中的交易所后缀，API只接受纯数字代码
            # 例如：将"000001.SZ"转换为"000001"
            ts_code_list_clean = [code.split('.')[0] for code in ts_code_list]
            logger.info(f"已清理股票代码格式，移除交易所后缀")
            
            logger.info(f"成功获取{len(ts_code_list)}支股票代码，开始分批获取实时数据")
            
            # 由于API限制一次最多20支股票，需要分批处理
            batch_size = 20
            all_results = []
            
            for i in range(0, len(ts_code_list_clean), batch_size):
                batch = ts_code_list_clean[i:i+batch_size]
                ts_code_list_batch = ts_code_list[i:i+batch_size]
                logger.info(f"正在处理批次 {i//batch_size + 1}/{(len(ts_code_list_clean)-1)//batch_size + 1}，包含{len(batch)}支股票")
                
                # 调用现有的getMyRealTimeDaily函数处理单个批次
                batch_result = self.getMyRealTimeDaily(batch)
                if batch_result is not None and not batch_result.empty:
                    # 添加股票代码
                    batch_result['ts_code'] = ts_code_list_batch
                    all_results.append(batch_result)
            
            # 合并所有批次的结果
            if all_results:
                final_result = DataFrame()
                for result in all_results:
                    final_result = final_result._append(result, ignore_index=True)
                
                logger.info(f"成功获取所有股票实时数据，共{len(final_result)}条记录")
                return final_result
            else:
                logger.warning("未能获取任何股票的实时数据")
                return None
                
        except Exception as e:
            logger.error("获取所有股票实时数据失败", exc_info=True)
            return None

