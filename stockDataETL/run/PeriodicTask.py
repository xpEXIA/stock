import asyncio
import json
from datetime import datetime
from django.http import JsonResponse
from stockDataETL import logger
from stockDataETL.dataTransform.commonUtils.get_trade_date import get_pretrade_date
# from stockDataETL.dataTransform.commonUtils.run_async import run_async
from stockDataETL.run.AsyncDailyTask import asyncDailyTask
# from asyncio import get_event_loop


async def periodicTask(request):
    """
    执行定期股票数据获取任务的HTTP接口

    Args:
        request: Django请求对象，包含以下参数：
            start: 开始日期，格式如'20260102'
            end: 结束日期，格式如'20260102'

    Returns:
        JsonResponse: 包含任务执行结果的JSON响应
    """
    try:
        # 从请求参数中获取日期
        start_date = request.GET.get('start')
        end_date = request.GET.get('end')

        # 验证必填参数
        if not start_date or not end_date:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "缺少start或end参数"
                }
            )

        # 将日期格式从20260102转换为2026-01-02
        try:
            start_date_formatted = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
            end_date_formatted = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "日期格式错误，请使用YYYYMMDD格式，例如：20260102"
                }
            )

        logger.info(f"开始执行定期任务，日期范围：{start_date_formatted} 至 {end_date_formatted}")

        # 获取全部交易日列表
        trade_date_list = get_pretrade_date(start_date_formatted, end_date_formatted)

        if not trade_date_list:
            return JsonResponse(
                {
                    "status": "error",
                    "message": f"在{start_date_formatted}至{end_date_formatted}范围内未找到交易日"
                }
            )

        logger.info(f"获取到{len(trade_date_list)}个交易日")

        # 遍历列表中的全部日期，调用AsyncDailyTask获取股票每日数据
        failure_dates = []
        for trade_date in trade_date_list:
            logger.info(f"开始处理交易日: {trade_date}")

            try:
                # 创建模拟请求对象，包含trade_date参数
                class MockRequest:
                    def __init__(self, trade_date):
                        self.GET = {'date': trade_date}

                # 调用AsyncDailyTask函数处理该交易日数据
                result = await asyncDailyTask(MockRequest(trade_date))
                status = json.loads(result.content.decode("utf-8"))['status']
                # loop = asyncio.get_running_loop()
                # result = loop.run_until_complete(asyncDailyTask(MockRequest(trade_date)))

                # 检查处理结果
                if status == 'success':
                    logger.info(f"交易日{trade_date}数据处理成功")
                else:
                    logger.error(f"交易日{trade_date}数据处理失败: {result.content.decode('utf-8')}")
                    failure_dates.append(trade_date)

            except Exception as e:
                logger.error(f"处理交易日{trade_date}时发生异常: {str(e)}")
                failure_dates.append(trade_date)

        if not failure_dates:
            logger.info("定期任务执行完成，所有交易日数据处理成功")
            return JsonResponse(
                {
                    "status": "success",
                    "message": f"定期任务执行完成，共处理{len(trade_date_list)}个交易日"
                }
            )
        else:
            logger.error(f"定期任务执行完成，{len(failure_dates)}个交易日处理失败，失败日期: {failure_dates}")
            return JsonResponse(
                {
                    "status": "error",
                    "message": f"定期任务执行完成，{len(failure_dates)}个交易日处理失败，失败日期: {failure_dates}"
                }
            )

    except Exception as e:
        logger.error(f"执行定期任务失败: {str(e)}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"执行失败: {str(e)}"
            }
        )
