# -*- coding: utf-8 -*-
"""多因子打分"""
from __future__ import annotations
import pandas as pd
import numpy as np

from src.factors import industry_momentum, size_factor

# --------------------- 因子列表 & 默认权重 ------------------------------
F = [
    "F_pe", "F_pb",
    "F_mom", "F_mom6m",
    "F_roa",
    "F_turn", "F_vol",
    "F_ind_mom", "F_mcap",
]

WEIGHTS = {
    "F_pe":    0.25,
    "F_pb":    0.00,
    "F_mom":   0.15,
    "F_mom6m": 0.15,
    "F_roa":   0.20,
    "F_turn":  0.05,
    "F_vol":   0.05,
    "F_ind_mom": 0.10,
    "F_mcap":  0.05,
}

# ------------------------ 工具函数 -------------------------------------
def _fill_na(df: pd.DataFrame, col: str) -> pd.Series:
    """缺失值用行业中位数回填，再用全市场中位数兜底"""
    if col not in df:
        return pd.Series(0, index=df.index)
    ser = df[col]
    if "industry" in df.columns:
        ser = ser.groupby(df["industry"]).transform(lambda s: s.fillna(s.median()))
    return ser.fillna(ser.median())

def _zscore(s: pd.Series) -> pd.Series:
    """Z-Score（加 epsilon 避免除零）"""
    eps = 1e-10
    std = s.std(ddof=0)
    if std < eps or np.isnan(std):
        return pd.Series(0, index=s.index)
    return (s - s.mean()) / (std + eps)

# ------------------------ 主函数 ---------------------------------------
def score(df: pd.DataFrame, w: dict[str, float] | None = None) -> pd.DataFrame:
    w = w or WEIGHTS

    df["F_pe"]   = -_zscore(_fill_na(df, "pe_ttm"))
    df["F_pb"]   = -_zscore(_fill_na(df, "pb"))
    df["F_mom"]  =  _zscore(_fill_na(df, "pct_chg_20d"))
    df["F_mom6m"] = _zscore(_fill_na(df, "pct_chg_126d"))
    df["F_roa"]  =  _zscore(_fill_na(df, "roa"))
    df["F_turn"] = -_zscore(_fill_na(df, "turnover_rate_f"))
    df["F_vol"]  = -_zscore(_fill_na(df, "vol_20d"))

    # 扩展因子
    df["F_ind_mom"] = industry_momentum(df)
    df["F_mcap"]    = size_factor(df)

    # 组合得分
    df["score"] = sum(df[f] * w.get(f, 0) for f in F)
    return df.sort_values("score", ascending=False)
