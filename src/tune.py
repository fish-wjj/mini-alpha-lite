# -*- coding: utf-8 -*-
"""
快速网格搜索 6 因子权重
- 仅抓一次 get_today_universe()，内存打分
- 默认 3^6 ≈ 729 组；如想更细可把 GRID 改成 [0.05,0.10,...]
"""
import itertools, json, numpy as np
from src.utils import get_today_universe
from src.factor_model import score, F_LIST

GRID = [0.05, 0.10, 0.15]      # 权重步长 5%
TOP_N = 50                      # 只看打分前 50 只

df = get_today_universe()       # ← 只拉一次

def sharp_ratio(series):
    return series.mean() / series.std(ddof=0) if series.std(ddof=0) else -9

best_w, best_s = None, -9

for comb in itertools.product(GRID, repeat=len(F_LIST)):
    if abs(sum(comb) - 1.0) > 1e-6:
        continue
    w = dict(zip(F_LIST, comb))
    top = score(df.copy(), w).head(TOP_N)
    s   = sharp_ratio(top["score"])
    if s > best_s:
        best_s, best_w = s, w

print("=== BEST WEIGHT (sum=1) ===")
print(json.dumps(best_w, indent=2))
