# -*- coding: utf-8 -*-
"""
因子打分（稳健版）
• 任何缺列自动生成全 0 Series，避免 AttributeError
"""
import pandas as pd, numpy as np

F_LIST = ["F_pe", "F_pb", "F_mom", "F_roa", "F_turn", "F_vol"]

# 可手动替换为 tune_fast.py 输出
WEIGHTS = {
    "F_pe":   0.43,
    "F_pb":   0.00,
    "F_mom":  0.14,
    "F_roa":  0.29,
    "F_turn": 0.00,
    "F_vol":  0.14,
}

def _ensure_series(df: pd.DataFrame, col: str) -> pd.Series:
    """若列不存在或全 NaN，返回全 0 Series，长度与 df 匹配"""
    if col not in df or df[col].isna().all():
        return pd.Series(0, index=df.index, dtype=float)
    return df[col].fillna(0)

def _z(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0, index=series.index)
    return (series - series.mean()) / std

def score(df: pd.DataFrame, weights: dict[str, float] | None = None) -> pd.DataFrame:
    w = weights or WEIGHTS

    df["F_pe"]   = -_z(_ensure_series(df, "pe_ttm"))
    df["F_pb"]   = -_z(_ensure_series(df, "pb"))
    df["F_mom"]  =  _z(_ensure_series(df, "pct_chg_20d"))
    df["F_roa"]  =  _z(_ensure_series(df, "roa"))
    df["F_turn"] = -_z(_ensure_series(df, "turnover_rate_f"))
    df["F_vol"]  = -_z(_ensure_series(df, "vol_20d"))

    df["score"] = sum(df[f] * w.get(f, 0) for f in F_LIST)
    return df.sort_values("score", ascending=False)
