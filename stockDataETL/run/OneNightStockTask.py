from django.http import JsonResponse
from stockDataETL import logger
from stockDataETL.tasks.dm_daily_one_night_stock_daily_task import dm_daily_one_night_stock_daily_task


def oneNightStockTask(request):
    """
    执行当日一夜情股票数据计算任务的HTTP接口
    
    Args:
        request: Django请求对象
        trade_date: 交易日，格式如'2025-12-05'，可选
    
    Returns:
        JsonResponse: 包含任务执行结果的JSON响应
    """
    try:
        # 从请求参数中获取trade_date
        trade_date = request.GET.get('trade_date')
        
        if not trade_date:
            return JsonResponse(
                {
                    "status": "error",
                    "message": "缺少trade_date参数"
                }
            )
        
        # 执行任务
        dm_daily_one_night_stock_daily_task(trade_date)
        
        return JsonResponse(
            {
                "status": "success",
                "message": f"当日一夜持股股票数据计算完成，交易日: {trade_date}"
            }
        )
    except Exception as e:
        logger.error(f"执行当日一夜持股股票数据计算任务失败: {str(e)}")
        return JsonResponse(
            {
                "status": "error",
                "message": f"执行失败: {str(e)}"
            }
        )
