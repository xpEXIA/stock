import requests
from django.db.models.expressions import result
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

    def getRealTimeDaily(self, ts_code_list: list = []) -> dict:

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



