# -*- coding: utf-8 -*-
import pandas as pd
F_LIST = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol"]

WEIGHTS = dict(zip(F_LIST,[0.20,0.20,0.20,0.15,0.15,0.10]))

z = lambda s: (s - s.mean()) / s.std(ddof=0)

def _safe(series: pd.Series, default=0):
    return series if series is not None else default

def score(df: pd.DataFrame, weights: dict[str,float]|None=None) -> pd.DataFrame:
    w = weights or WEIGHTS
    df["F_pe"]   = -z(df["pe_ttm"])
    df["F_pb"]   = -z(df["pb"])
    df["F_mom"]  =  z(df["pct_chg_20d"])
    df["F_roa"]  =  z(_safe(df.get("roa")))
    df["F_turn"] = -z(_safe(df.get("turnover_rate_f")))
    df["F_vol"]  = -z(_safe(df.get("vol_20d")))
    df["score"]  = sum(df[k]*w[k] for k in w)
    return df.sort_values("score", ascending=False)
