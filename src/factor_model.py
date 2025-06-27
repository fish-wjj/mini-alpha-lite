# -*- coding: utf-8 -*-
"""3 因子打分：估值 PE/PB + 20d 动量"""
import pandas as pd


def score(df: pd.DataFrame) -> pd.DataFrame:
    z = lambda s: (s - s.mean()) / s.std(ddof=0)

    df["F_pe"] = -z(df["pe_ttm"])
    df["F_pb"] = -z(df["pb"])
    df["F_mom"] = z(df["pct_chg_20d"])

    weights = {"F_pe": 0.5, "F_pb": 0.3, "F_mom": 0.2}
    df["score"] = sum(df[k] * w for k, w in weights.items())

    return df.sort_values("score", ascending=False)
