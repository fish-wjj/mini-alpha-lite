# -*- coding: utf-8 -*-
"""
6 因子打分（调权后版本）
"""
import pandas as pd, numpy as np

F = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol"]

# ◆ 调高动量、换手；下调估值 ◆
WEIGHTS = {
    "F_pe":   0.30,
    "F_pb":   0.05,
    "F_mom":  0.20,
    "F_roa":  0.25,
    "F_turn": 0.10,
    "F_vol":  0.10,
}

def _safe(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df or df[col].isna().all():
        return pd.Series(0, index=df.index, dtype=float)
    return df[col].fillna(0)

def _z(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0, index=s.index)
    return (s - s.mean()) / std

def score(df: pd.DataFrame, w: dict[str, float] | None = None) -> pd.DataFrame:
    w = w or WEIGHTS
    df["F_pe"]   = -_z(_safe(df, "pe_ttm"))
    df["F_pb"]   = -_z(_safe(df, "pb"))
    df["F_mom"]  =  _z(_safe(df, "pct_chg_20d"))
    df["F_roa"]  =  _z(_safe(df, "roa"))
    df["F_turn"] = -_z(_safe(df, "turnover_rate_f"))
    df["F_vol"]  = -_z(_safe(df, "vol_20d"))
    df["score"]  = sum(df[f] * w.get(f, 0) for f in F)
    return df.sort_values("score", ascending=False)
