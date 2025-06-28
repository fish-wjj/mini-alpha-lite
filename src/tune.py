# -*- coding: utf-8 -*-
import itertools, json, numpy as np, pandas as pd
from src.utils import get_today_universe
from src.factor_model import score, F_LIST

GRID = [0, 0.05, 0.10, 0.15, 0.20, 0.25]
TOP_N = 50

df_base = get_today_universe()

def sharp(series: pd.Series) -> float:
    m, s = series.mean(skipna=True), series.std(ddof=0, skipna=True)
    return -9 if s == 0 or np.isnan(s) else m / s

best_w, best_s = None, -9

for raw in itertools.product(GRID, repeat=len(F_LIST)):
    if not any(raw):          # 全 0 跳过
        continue
    w = dict(zip(F_LIST, raw))
    total = sum(w.values())
    w = {k: v/total for k, v in w.items()}   # 归一

    top = score(df_base.copy(), w).head(TOP_N)["score"]
    s   = sharp(top)
    if s > best_s:
        best_s, best_w = s, w

print("=== BEST WEIGHT (sum=1) ===")
print(json.dumps(best_w, indent=2, ensure_ascii=False))
