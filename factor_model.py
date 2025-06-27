# -*- coding: utf-8 -*-
"""
极简 3 因子打分：
  • F_pe   : 估值（越低越好）
  • F_pb   : 估值（越低越好）
  • F_mom  : 20 日动量（越高越好）
最终得分 = 0.5*F_pe + 0.3*F_pb + 0.2*F_mom
"""
import pandas as pd

def score(df: pd.DataFrame) -> pd.DataFrame:
    z = lambda s: (s - s.mean()) / s.std()

    df["F_pe"]  = -z(df["pe_ttm"])
    df["F_pb"]  = -z(df["pb"])
    df["F_mom"] =  z(df["pct_chg_20d"])

    weight = {"F_pe": 0.5, "F_pb": 0.3, "F_mom": 0.2}
    df["score"] = sum(df[k] * w for k, w in weight.items())

    return df.sort_values("score", ascending=False)
