"""
统一入口：一个进程同时启动两个服务

  - 行情服务（主线程）：TqSdk → Redis Hash + Pub/Sub
  - MQTT 价差服务（子线程）：Redis 行情 → 计算跨期价差 → Mosquitto 分发

用法: python main.py
两个服务也可分别单独运行：python run.py / python mqtt_service.py
"""

import asyncio
import logging
import threading
import time

import mqtt_service
from run import start_market_service


def start_mqtt_service():
    """在守护线程中运行 MQTT 价差服务的 asyncio 事件循环，返回 stop() 清理函数"""
    holder = {}
    state = {"stopping": False}
    box = {"task": None}

    def runner():
        # paho-mqtt 依赖 add_reader，Windows 默认 Proactor 循环不支持，统一用 Selector
        loop = asyncio.SelectorEventLoop()
        asyncio.set_event_loop(loop)

        async def _run():
            holder["loop"] = loop
            await mqtt_service.main()

        box["task"] = loop.create_task(_run())
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

    def stop():
        state["stopping"] = True
        # 等待事件循环就绪（覆盖启动后立即退出的场景）
        for _ in range(50):
            if holder.get("loop") is not None or not thread.is_alive():
                break
            time.sleep(0.1)
        loop = holder.get("loop")
        if loop and loop.is_running():
            async def _cancel():
                task = box["task"]
                if task:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
                loop.stop()
            asyncio.run_coroutine_threadsafe(_cancel(), loop)
        thread.join(timeout=5)

    thread = threading.Thread(target=runner, name="mqtt-service", daemon=True)
    thread.start()
    return stop


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    stop_mqtt = start_mqtt_service()
    logging.info("=" * 60)
    try:
        start_market_service()  # 阻塞主线程直至退出
    except KeyboardInterrupt:
        logging.info("用户中断")
    except Exception:
        logging.exception("运行异常")
    finally:
        stop_mqtt()
        logging.info("已全部退出")


if __name__ == "__main__":
    main()
