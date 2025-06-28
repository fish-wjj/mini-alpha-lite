# -*- coding: utf-8 -*-
import pandas as pd, numpy as np
F = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol"]
WEIGHTS = {"F_pe":0.43,"F_pb":0.0,"F_mom":0.14,
           "F_roa":0.29,"F_turn":0.0,"F_vol":0.14}

def _safe(df,col): return df[col].fillna(0) if col in df else pd.Series(0,index=df.index)
def _z(s): std=s.std(ddof=0); return pd.Series(0,index=s.index) if std==0 or np.isnan(std) else (s-s.mean())/std

def score(df:pd.DataFrame, w:dict[str,float]|None=None)->pd.DataFrame:
    w=w or WEIGHTS
    df["F_pe"]  = -_z(_safe(df,"pe_ttm"))
    df["F_pb"]  = -_z(_safe(df,"pb"))
    df["F_mom"] =  _z(_safe(df,"pct_chg_20d"))
    df["F_roa"] =  _z(_safe(df,"roa"))
    df["F_turn"]= -_z(_safe(df,"turnover_rate_f"))
    df["F_vol"] = -_z(_safe(df,"vol_20d"))
    df["score"] = sum(df[f]*w.get(f,0) for f in F)
    return df.sort_values("score",ascending=False)
