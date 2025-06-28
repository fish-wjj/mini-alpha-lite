# -*- coding: utf-8 -*-
"""
读取 config.yaml
把所有数值字段自动转为 float/int，避免 YAML 写成字符串时的类型错误
"""
import yaml
from pathlib import Path

_CFG_PATH = Path(__file__).resolve().parent.parent / "config.yaml"

_NUMERIC_KEYS = {
    "cash": float,
    "core_ratio": float,
    "alpha_ratio": float,
    "num_alpha": int,
    "stop_loss": float,
    "max_drawdown": float,
    "min_amount": float,
    "lot": int,
}

def load_cfg() -> dict:
    with open(_CFG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    for k, cast in _NUMERIC_KEYS.items():
        if k in cfg:
            try:
                cfg[k] = cast(cfg[k])
            except (TypeError, ValueError):
                raise ValueError(f"config.yaml 字段 {k} 必须是 {_NUMERIC_KEYS[k].__name__}")

    return cfg
