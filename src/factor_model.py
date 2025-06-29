#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
打分函数：给入 dataframe（由 utils.build_today_universe() 生成），返回因子分数并排序
"""
from __future__ import annotations
import numpy as np
import pandas as pd

# 默认权重
WEIGHTS = dict(
    F_pe=0.3,
    F_pb=0.1,
    F_mom=0.15,
    F_roa=0.25,
    F_turn=-0.05,
    F_vol=-0.05,
    F_size=-0.05,
)
F_LIST = list(WEIGHTS.keys())


def _safe(df: pd.DataFrame, col: str) -> pd.Series:
    """缺失值 → 全市场中位数；若整列缺失则 0"""
    if col not in df or df[col].isna().all():
        return pd.Series(0, index=df.index)
    med = df[col].median()
    return df[col].fillna(med)


def _z(s: pd.Series) -> pd.Series:
    """Z-Score 标准化，避免 std≈0"""
    std = s.std(ddof=0)
    if std < 1e-9 or np.isnan(std):
        return pd.Series(0, index=s.index)
    return (s - s.mean()) / std


def score(df: pd.DataFrame, w: dict[str, float] | None = None) -> pd.DataFrame:
    w = w or WEIGHTS
    df = df.copy()

    df["F_pe"] = -_z(_safe(df, "pe_ttm"))
    df["F_pb"] = -_z(_safe(df, "pb"))
    df["F_mom"] = _z(_safe(df, "pct_chg_20d"))
    df["F_roa"] = _z(_safe(df, "roa"))
    df["F_turn"] = -_z(_safe(df, "turnover_rate_f"))
    df["F_vol"] = -_z(_safe(df, "vol_20d"))
    df["F_size"] = -_z(_safe(df, "total_mv").apply(np.log1p))

    df["score"] = sum(df[f] * w.get(f, 0) for f in F_LIST)
    return df.sort_values("score", ascending=False)
