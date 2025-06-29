# -*- coding: utf-8 -*-
"""行业动量 & 市值因子"""
from __future__ import annotations
import pandas as pd
import numpy as np

def _z(s: pd.Series) -> pd.Series:
    eps = 1e-10
    std = s.std(ddof=0)
    return (s - s.mean()) / (std + eps) if std > eps else pd.Series(0, index=s.index)

def industry_momentum(df: pd.DataFrame) -> pd.Series:
    """
    按行业计算过去 20 日涨跌幅的行业平均，再对结果做 Z 标准化。
    如果数据缺失返回 0。
    """
    if "industry" not in df or "pct_chg_20d" not in df:
        return pd.Series(0, index=df.index)

    ind_mom = (df.groupby("industry")["pct_chg_20d"]
                 .transform("mean"))
    return _z(ind_mom)

def size_factor(df: pd.DataFrame) -> pd.Series:
    """对数市值取反向 Z 分数（小市值打高分）"""
    if "total_mv" not in df:
        return pd.Series(0, index=df.index)
    mv = df["total_mv"].replace(0, np.nan)
    mv = mv.fillna(mv.median())
    return -_z(np.log(mv))
