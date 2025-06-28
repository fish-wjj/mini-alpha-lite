# -*- coding: utf-8 -*-
"""
6 因子打分
  F_pe      : 估值（PE 低好）
  F_pb      : 估值（PB 低好）
  F_mom     : 20 日动量
  F_roa     : ROA 质优
  F_turn    : 换手率低 (稳健资金)
  F_vol     : 20 日波动率低
默认权重见 WEIGHTS，可由 tune.py 网格搜索后改写
"""
import pandas as pd

WEIGHTS = {
    "F_pe":    0.20,
    "F_pb":    0.20,
    "F_mom":   0.20,
    "F_roa":   0.15,
    "F_turn":  0.15,
    "F_vol":   0.10,
}

def z(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std(ddof=0)

def score(df: pd.DataFrame, weights: dict[str, float] | None = None) -> pd.DataFrame:
    w = weights or WEIGHTS

    df["F_pe"]   = -z(df["pe_ttm"])
    df["F_pb"]   = -z(df["pb"])
    df["F_mom"]  =  z(df["pct_chg_20d"])
    df["F_roa"]  =  z(df["roa"])
    df["F_turn"] = -z(df["turnover_rate_f"])
    df["F_vol"]  = -z(df["vol_20d"])

    df["score"] = sum(df[k] * w.get(k, 0) for k in w)
    return df.sort_values("score", ascending=False)
