import requests
import time
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
            # 添加超时参数，防止请求长时间等待
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                logger.error(f"request real time daily data failed with status code: {response.status_code}, "
                             + f"reason: {response.reason}")
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
                                "t": "update",
                                "pe": "pe",
                                "tr": "turnover_rate_f",
                                "pd_ratio": "pd_ratio",
                                "tv": "t_vol",})
            )
            return result
        except requests.exceptions.Timeout as e:
            logger.error(f"Real time daily API request timed out: {str(e)}")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Real time daily API connection error: {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Real time daily API request exception: {str(e)}")
            return None
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
                
                # 调用现有的getMyRealTimeDaily函数处理单个批次，最多重试3次
                retry_count = 0
                max_retries = 3
                batch_result = None
                
                while retry_count < max_retries:
                    batch_result = self.getMyRealTimeDaily(batch)
                    if batch_result is not None and not batch_result.empty:
                        break
                    
                    retry_count += 1
                    logger.warning(f"批次 {i//batch_size + 1} 获取实时数据失败，正在进行第 {retry_count} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
                
                if batch_result is not None and not batch_result.empty:
                    # 添加股票代码
                    batch_result['ts_code'] = ts_code_list_batch
                    all_results.append(batch_result)
                else:
                    logger.error(f"批次 {i//batch_size + 1} 获取实时数据失败，已达到最大重试次数 {max_retries} 次")
            
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

    def getMyHistoryTime(self, ts_code: str,  start_time: str, end_time: str,time_level: str = '30') -> DataFrame:

        """
        获取MyData历史分时数据
        API接口：https://api.mairuiapi.com/hsstock/history/股票代码.市场（如000001.SZ）/分时级别(如d)/除权方式/您的licence?st=开始时间(如20240601)&et=结束时间(如20250430)&lt=最新条数(如100)
        演示URL：https://api.mairuiapi.com/hsstock/history/000001.SZ/d/n/LICENCE-66D8-9F96-0C7F0FBCD073?st=20250101&et=20250430&lt=100
        :param ts_code: 股票代码，仅能传一个
        :param time_level: 目前分时级别支持5分钟、15分钟、30分钟、60分钟、日线、周线、月线、
        年线，对应的请求参数分别为5、15、30、60、d、w、m、y
        :param start_time: 开始时间
        :param end_time: 结束时间
        开始时间以及结束时间的格式均为 YYYYMMDD 或 YYYYMMDDhhmmss，例如：'20240101' 或'20241231235959'
        :return: 个股历史分时数据，
        数据格式如下:
        [{"t":"2025-12-23 10:00:00","o":11.52,"h":11.59,"l":11.5,"c":11.52,"v":172994.0,"a":199685294.58,"pc":11.52,"sf":0.0},
        {"t":"2025-12-23 10:30:00","o":11.52,"h":11.6,"l":11.51,"c":11.58,"v":157022.0,"a":181697451.86,"pc":11.52,"sf":0.0}]
        数据字典如下：
        字段名称	数据类型	字段说明
        t	string	交易时间
        o	float	开盘价
        h	float	最高价
        l	float	最低价
        c	float	收盘价
        v	float	成交量
        a	float	成交额
        pc	float	前收盘价
        sf	int	停牌 1停牌，0 不停牌
        """
        # 验证参数
        if not ts_code:
            logger.error("股票代码不能为空")
            return None
        
        # 支持的时间级别列表
        valid_time_levels = ['5', '15', '30', '60', 'd', 'w', 'm', 'y']
        if time_level not in valid_time_levels:
            logger.error(f"无效的时间级别：{time_level}，支持的级别为：{valid_time_levels}")
            return None
        
        # 除权方式默认为"n"（不除权），最新条数默认为100
        adjust_flag = "n"
        # limit = "100"
        
        # 构建URL
        url = (
            f"https://api.mairuiapi.com/hsstock/history/{ts_code}/{time_level}/{adjust_flag}/{self.license_key}" 
            f"?st={start_time}&et={end_time}"
            # f"&lt={limit}"
        )
        
        # 设置自动重试机制，最多重试3次
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                logger.info(f"获取{ts_code}的历史分时数据，时间级别：{time_level}，第 {retry_count + 1} 次尝试")
                
                # 发送请求，设置超时
                response = requests.get(url, timeout=10)
                
                # 检查响应状态码
                if response.status_code != 200:
                    logger.error(f"请求历史分时数据失败，状态码：{response.status_code}，原因：{response.reason}")
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                        time.sleep(1)  # 等待1秒后重试
                    continue
                
                # 检查响应内容是否为空
                if not response.text.strip():
                    logger.error("历史分时API返回空响应")
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                        time.sleep(1)  # 等待1秒后重试
                    continue
                
                # 解析JSON响应
                data = response.json()
                if not data:
                    logger.error("历史分时API返回空数据")
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                        time.sleep(1)  # 等待1秒后重试
                    continue
                
                # 转换为DataFrame并映射列名
                df = DataFrame(data)
                df = df.rename(columns={
                    "t": "trade_time",
                    "o": "open",
                    "h": "high",
                    "l": "low",
                    "c": "close",
                    "v": "vol",
                    "a": "amount",
                    "pc": "pre_close",
                    "sf": "suspension_flag"
                })
                
                # 添加股票代码和时间级别信息
                df['ts_code'] = ts_code
                df['time_level'] = time_level
                
                logger.info(f"成功获取{ts_code}的历史分时数据，共{len(df)}条记录")
                return df
                
            except requests.exceptions.Timeout as e:
                logger.error(f"历史分时API请求超时：{str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
            except requests.exceptions.ConnectionError as e:
                logger.error(f"历史分时API连接错误：{str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
            except requests.exceptions.RequestException as e:
                logger.error(f"历史分时API请求异常：{str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
            except ValueError as e:
                logger.error(f"历史分时API响应解析错误：{str(e)}")
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
            except Exception as e:
                logger.error(f"获取历史分时数据失败：{str(e)}", exc_info=True)
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"正在进行第 {retry_count + 1} 次重试...")
                    time.sleep(1)  # 等待1秒后重试
        
        # 达到最大重试次数仍未成功
        logger.error(f"获取{ts_code}的历史分时数据失败，已达到最大重试次数 {max_retries} 次")
        return None