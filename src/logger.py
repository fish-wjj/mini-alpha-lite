# -*- coding: utf-8 -*-
"""
统一日志封装
写入 logs/runtime.log，按 10 MB 滚动，保留 7 天
"""
from loguru import logger
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger.add(
    LOG_DIR / "runtime.log",
    rotation="10 MB",
    retention="7 days",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level} | {message}",
    enqueue=True,      # 多线程安全
)
