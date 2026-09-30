#!/bin/bash
# 使用 screen 管理 Python 后台服务
# 用法: ./run_service.sh {start|stop|restart|status|attach}

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$APP_DIR"

SESSION_NAME="tq_md_app"   # screen 会话名称，可自定义
VENV_DIR=".venv"
PYTHON_SCRIPT="main.py"    # 统一入口：行情服务 + WS 订阅服务

# 检查 screen 是否安装
if ! command -v screen &> /dev/null; then
    echo "错误: screen 未安装，请先安装: sudo apt install screen"
    exit 1
fi

# 检查虚拟环境
if [ ! -d "$VENV_DIR" ]; then
    echo "错误: 虚拟环境 '$VENV_DIR' 不存在"
    exit 1
fi

# 检查 Python 脚本
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "错误: Python 脚本 '$PYTHON_SCRIPT' 不存在"
    exit 1
fi

start() {
    # 检查是否已存在同名会话
    if screen -list | grep -q "\.${SESSION_NAME}\s"; then
        echo "服务已在运行（会话: $SESSION_NAME）"
        return 1
    fi

    echo "启动服务（screen 会话: $SESSION_NAME）..."
    # 创建新 screen 会话，并执行激活虚拟环境 + 运行脚本
    screen -dmS "$SESSION_NAME" bash -c "
        source '$VENV_DIR/bin/activate'
        python '$PYTHON_SCRIPT'
        exec bash   # 程序结束后保留终端，便于查看错误
    "
    sleep 1
    if screen -list | grep -q "\.${SESSION_NAME}\s"; then
        echo "服务已启动，使用 './run_service.sh attach' 进入查看输出"
    else
        echo "启动失败，请检查脚本或依赖"
    fi
}

stop() {
    if ! screen -list | grep -q "\.${SESSION_NAME}\s"; then
        echo "服务未运行"
        return 1
    fi
    echo "停止服务（会话: $SESSION_NAME）..."
    screen -S "$SESSION_NAME" -X quit
    echo "服务已停止"
}

status() {
    if screen -list | grep -q "\.${SESSION_NAME}\s"; then
        echo "服务正在运行（会话: $SESSION_NAME）"
        # 显示会话创建时间等信息（可选）
        screen -list | grep "\.${SESSION_NAME}\s"
    else
        echo "服务未运行"
    fi
}

attach() {
    if ! screen -list | grep -q "\.${SESSION_NAME}\s"; then
        echo "服务未运行，请先启动"
        return 1
    fi
    echo "正在进入会话 $SESSION_NAME（脱离请按 Ctrl+A D）..."
    screen -r "$SESSION_NAME"
}

restart() {
    stop
    sleep 1
    start
}

case "$1" in
    start)   start   ;;
    stop)    stop    ;;
    restart) restart ;;
    status)  status  ;;
    attach)  attach  ;;
    *)
        echo "用法: $0 {start|stop|restart|status|attach}"
        exit 1
        ;;
esac