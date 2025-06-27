# -*- coding: utf-8 -*-
from loguru import logger
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logger.add(
    LOG_DIR / "runtime.log",
    rotation="10 MB",
    retention="7 days",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {level} | {message}",
    enqueue=True,
)
