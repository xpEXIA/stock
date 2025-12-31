from django.http import JsonResponse
from stockDataETL import logger
from stockDataETL.tasks.dm_daily_one_night_stock_history_task import dm_daily_one_night_stock_history_task


def oneNightStockHistoryTask(request):
    """
    执行一夜持股历史数据计算任务的HTTP接口
    
    Args:
        request: Django请求对象，包含以下参数：
            start_time: 开始时间，例如：'20240101' 或'20241231235959'
            end_time: 结束时间，例如：'20240101' 或'20241231235959'
            period: 历史成交量计算周期，默认为过去5个交易日
    
    Returns:
        JsonResponse: 包含任务执行结果的JSON响应
    """
    try:
        # 从请求参数中获取参数
        start_time = request.GET.get('start_time')
        end_time = request.GET.get('end_time')
        period = request.GET.get('period', 5)
        
        # 验证必填参数
        if not start_time or not end_time:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "缺少start_time或end_time参数"
                }
            )
        
        # 将period转换为整数
        try:
            period = int(period)
        except ValueError:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "period参数必须为整数"
                }
            )
        
        # 执行任务
        dm_daily_one_night_stock_history_task(start_time, end_time, period)
        
        return JsonResponse(
            {
                "status": "success",
                "message": f"一夜持股历史数据计算完成，开始时间: {start_time}, 结束时间: {end_time}"
            }
        )
    except Exception as e:
        logger.error(f"执行一夜持股历史数据计算任务失败: {str(e)}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"执行失败: {str(e)}"
            }
        )
