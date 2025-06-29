# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
from src.factors import industry_momentum, size_factor

F = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol","F_ind_mom","F_mcap"]
WEIGHTS = {
    "F_pe":   0.25,
    "F_pb":   0.05,
    "F_mom":  0.25,
    "F_roa":  0.15,
    "F_turn": 0.10,
    "F_vol":  0.05,
    "F_ind_mom": 0.10,
    "F_mcap":    0.05,
}

def _safe(df, col): return df[col].fillna(0) if col in df else pd.Series(0, index=df.index)

def _z(s: pd.Series) -> pd.Series:
    """Z-Score，加入 epsilon 避免 std→0"""
    std = s.std(ddof=0)
    eps = 1e-10
    if std < eps or np.isnan(std):
        return pd.Series(0, index=s.index)
    return (s - s.mean()) / (std + eps)

def score(df: pd.DataFrame, w: dict[str, float] | None = None) -> pd.DataFrame:
    w = w or WEIGHTS
    df["F_pe"]      = -_z(_safe(df, "pe_ttm"))
    df["F_pb"]      = -_z(_safe(df, "pb"))
    df["F_mom"]     =  _z(_safe(df, "pct_chg_20d"))
    df["F_roa"]     =  _z(_safe(df, "roa"))
    df["F_turn"]    = -_z(_safe(df, "turnover_rate_f"))
    df["F_vol"]     = -_z(_safe(df, "vol_20d"))
    df["F_ind_mom"] =  _z(industry_momentum(df))
    df["F_mcap"]    =  _z(size_factor(df))
    df["score"] = sum(df[f] * w.get(f, 0) for f in F)
    return df.sort_values("score", ascending=False)
