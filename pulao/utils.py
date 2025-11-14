import logging


def enable_console_log():
    logger = logging.getLogger()

    # 创建 console 输出 handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 格式
    formatter = logging.Formatter("%(asctime)s  %(levelname)s  %(message)s")
    console_handler.setFormatter(formatter)

    # 避免重复添加
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(console_handler)
