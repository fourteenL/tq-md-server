"""
行情服务入口：TqSdk 行情 → Redis
用法: python run.py
（统一启动两个服务请用 python main.py）
"""

import logging
import redis
from tqsdk import TqApi, TqAuth

from config import TQ_CONFIG, REDIS_CONFIG
from core.quote_manager import run_update_loop


def start_market_service() -> None:
    """初始化 TqSdk 与 Redis，阻塞运行行情循环；任何退出路径均释放连接"""
    api = TqApi(auth=TqAuth(**TQ_CONFIG))
    redis_db = redis.Redis(**REDIS_CONFIG, decode_responses=True)
    try:
        if redis_db.ping():
            logging.info("Redis 已连接")
        run_update_loop(api, redis_db)
    finally:
        logging.info("正在关闭行情服务...")
        api.close()
        redis_db.close()
        logging.info("行情服务已退出")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    try:
        start_market_service()
    except KeyboardInterrupt:
        logging.info("用户中断")
    except Exception:
        logging.exception("运行异常")
