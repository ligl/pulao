import atexit
import json
import os
import logging
import signal
import sys
from logging.handlers import QueueHandler, QueueListener, TimedRotatingFileHandler
from queue import Queue
import structlog
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
from structlog.stdlib import BoundLogger


# -------------------------
# 1️⃣ 队列 + 文件配置
# -------------------------
def init_logging(log_dir="logs", level=logging.INFO):
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "pulao.log")

    # 标准 logging handler
    # file_handler = TimedRotatingFileHandler(
    #     filename=log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    # )
    file_handler = ConcurrentTimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        utc=False,
    )
    console_handler = logging.StreamHandler(sys.stdout)

    # 队列异步写日志
    log_queue = Queue(-1)
    queue_handler = QueueHandler(log_queue)
    listener = QueueListener(log_queue, file_handler, console_handler)
    listener.start()

    # 配置 root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)
    root_logger.propagate = False

    # -------------------------
    # 2️⃣ structlog 配置
    # -------------------------
    structlog.configure(
        processors=[
            # 添加标准字段
            structlog.stdlib.add_log_level,  # level name
            structlog.stdlib.add_logger_name,  # logger name / module
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", key="time", utc=False),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(
                serializer=lambda obj, **kw: json.dumps(obj, ensure_ascii=False, **kw)
            ),  # 输出 JSON
        ],
        context_class=dict,  # 用户 key=value 会存到 context
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    def close_listener():
        listener.stop()

    # 退出回调
    atexit.register(close_listener)

    # 处理 SIGTERM / SIGINT
    def cleanup(signum, frame):
        close_listener()
        sys.exit(0)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    return listener


# -------------------------
# 3️⃣ 获取 logger
# -------------------------
def get_logger(name:str = None):
    return structlog.get_logger(name)

