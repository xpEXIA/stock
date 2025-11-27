#!/bin/bash

# Django项目启动脚本 (Shell)
# 使用Gunicorn作为WSGI服务器

# 设置错误时退出
set -e

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 虚拟环境路径
VENV_PATH="$PROJECT_ROOT/.venv"
VENV_ACTIVATE="$VENV_PATH/bin/activate"

# Django设置模块
DJANGO_SETTINGS_MODULE="stockDataETL.settings"

# Gunicorn配置
GUNICORN_HOST="0.0.0.0"
GUNICORN_PORT="12037"
GUNICORN_WORKERS=4  # 工作进程数，通常设置为CPU核心数的2倍
GUNICORN_TIMEOUT=300
GUNICORN_LOG_LEVEL="info"
GUNICORN_LOG_FILE="$PROJECT_ROOT/gunicorn.log"
GUNICORN_PID_FILE="$PROJECT_ROOT/gunicorn.pid"

# 显示启动信息
echo "====================================="
echo "Django项目启动脚本 (Gunicorn)"
echo "====================================="
echo "项目根目录: $PROJECT_ROOT"
echo "虚拟环境: $VENV_PATH"
echo "监听地址: $GUNICORN_HOST:$GUNICORN_PORT"
echo "====================================="

# 检查虚拟环境是否存在
if [ ! -d "$VENV_PATH" ]; then
    echo "错误: 虚拟环境不存在!" >&2
    echo "请先创建虚拟环境: python3 -m venv .venv" >&2
    exit 1
fi

# 激活虚拟环境
echo "激活虚拟环境..."
source "$VENV_ACTIVATE"
echo "虚拟环境激活成功!"

# 检查gunicorn是否已安装
echo "检查gunicorn是否已安装..."
gunicorn_version=$(python -c "import gunicorn; print(gunicorn.__version__)" 2>/dev/null)
if [ -z "$gunicorn_version" ]; then
    echo "警告: gunicorn未安装，尝试安装..."
    if ! python -m pip install gunicorn; then
        echo "错误: 无法安装gunicorn!" >&2
        exit 1
    fi
    echo "gunicorn安装成功!"
else
    echo "gunicorn版本: $gunicorn_version"
fi

# 检查项目是否已安装
echo "检查项目依赖是否已安装..."
if ! python -c "import stockDataETL" 2>/dev/null; then
    echo "警告: 项目依赖可能未完全安装，尝试安装..."
    # 检查requirements.txt是否存在
    if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
        python -m pip install -r "$PROJECT_ROOT/requirements.txt"
        echo "项目依赖安装成功!"
    else
        echo "警告: requirements.txt文件不存在!"
    fi
else
    echo "项目依赖检查通过!"
fi

# 执行数据库迁移
echo "执行数据库迁移..."
cd "$PROJECT_ROOT"
if ! python manage.py migrate --noinput; then
    echo "警告: 数据库迁移时出错，但将继续尝试启动..."
else
    echo "数据库迁移完成!"
fi

# 收集静态文件
echo "收集静态文件..."
if ! python manage.py collectstatic --noinput; then
    echo "警告: 收集静态文件时出错，但将继续尝试启动..."
else
    echo "静态文件收集完成!"
fi

# 启动Gunicorn服务器
echo "====================================="
echo "正在启动Gunicorn服务器..."
echo "日志文件: $GUNICORN_LOG_FILE"
echo "按 Ctrl+C 停止服务器"
echo "====================================="

# 设置环境变量
export DJANGO_SETTINGS_MODULE="$DJANGO_SETTINGS_MODULE"

# 启动Gunicorn
gunicorn stockDataETL.wsgi:application \
    --bind "$GUNICORN_HOST:$GUNICORN_PORT" \
    --workers $GUNICORN_WORKERS \
    --timeout $GUNICORN_TIMEOUT \
    --log-level $GUNICORN_LOG_LEVEL \
    --log-file "$GUNICORN_LOG_FILE" \
    --pid "$GUNICORN_PID_FILE"

# 捕获退出信号并清理
cleanup() {
    echo "正在停止Gunicorn服务器..."
    if [ -f "$GUNICORN_PID_FILE" ]; then
        kill $(cat "$GUNICORN_PID_FILE")
        rm -f "$GUNICORN_PID_FILE"
    fi
    echo "Gunicorn服务器已停止"
}

trap cleanup SIGINT SIGTERM

# 等待服务器退出
wait