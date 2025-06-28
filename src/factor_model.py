# -*- coding: utf-8 -*-
import pandas as pd, numpy as np

F_LIST = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol"]

WEIGHTS = dict(zip(F_LIST,[0.20,0.20,0.20,0.15,0.15,0.10]))

def z(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0, index=s.index)
    return (s - s.mean()) / std

def score(df: pd.DataFrame, weights: dict[str,float] | None = None) -> pd.DataFrame:
    w = weights or WEIGHTS

    df["F_pe"]   = -z(df["pe_ttm"])
    df["F_pb"]   = -z(df["pb"])
    df["F_mom"]  =  z(df["pct_chg_20d"])
    df["F_roa"]  =  z(df.get("roa", 0))
    df["F_turn"] = -z(df.get("turnover_rate_f", 0))
    df["F_vol"]  = -z(df.get("vol_20d", 0))

    df["score"]  = sum(df[f] * w.get(f,0) for f in F_LIST)
    df["score"].fillna(0, inplace=True)
    return df.sort_values("score", ascending=False)
