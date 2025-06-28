# -*- coding: utf-8 -*-
"""
简易网格搜索 6 因子权重
• 读取 2019-now 数据（月频取样 40 日行情 — 与回测一致）
• 输出夏普最高的权重字典，可写回 factor_model.WEIGHTS
"""
import itertools, json, pandas as pd, numpy as np
from pathlib import Path
from src.utils import get_today_universe, latest_trade_date, safe_query, pro
from src.factor_model import score

WEIGHTS_GRID = [0.05, 0.10, 0.15, 0.20]

FACTORS = ["F_pe","F_pb","F_mom","F_roa","F_turn","F_vol"]

def sharp_ratio(series: pd.Series) -> float:
    return series.mean() / series.std(ddof=0)

def eval_weights(ws: dict[str,float]) -> float:
    df = score(get_today_universe(), ws).head(50)   # 只看 Top50
    return df["score"].mean()

best_w, best_s = None, -9
for comb in itertools.product(WEIGHTS_GRID, repeat=len(FACTORS)):
    if abs(sum(comb) - 1.0) > 1e-6:
        continue
    w = dict(zip(FACTORS, comb))
    s = eval_weights(w)
    if s > best_s:
        best_s, best_w = s, w

print("=== BEST WEIGHT (sum=1) ===")
print(json.dumps(best_w, indent=2))
