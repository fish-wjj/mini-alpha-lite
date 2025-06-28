# -*- coding: utf-8 -*-
"""
超快网格搜索 6 因子权重（46k 组合≈5 秒）
"""
import itertools, json, numpy as np
from src.utils import get_today_universe
from src.factor_model import F_LIST, z

GRID = [0, 0.05, 0.10, 0.15, 0.20, 0.25]
TOP_N = 50

# ① 预计算 6 因子矩阵 F
df = get_today_universe()
F = np.column_stack([
    -z(df["pe_ttm"]),
    -z(df["pb"]),
     z(df["pct_chg_20d"]),
     z(df["roa"]),
    -z(df["turnover_rate_f"]),
    -z(df["vol_20d"]),
]).astype(np.float32)        # shape=(5405,6)

# ② 构造权重网格矩阵 W  (归一化)
W_raw = np.array([w for w in itertools.product(GRID, repeat=6) if any(w)], dtype=np.float32)
W = W_raw / W_raw.sum(axis=1, keepdims=True)

# ③ 全组合得分矩阵  S = F @ W.T
S = F @ W.T                # shape=(5405, 46656)

# ④ 取每列 Top-50 的得分并算夏普
idx_top = np.argpartition(S, -TOP_N, axis=0)[-TOP_N:]   # indices of Top-50
top_scores = np.take_along_axis(S, idx_top, axis=0)     # shape=(50,46656)
mean = top_scores.mean(axis=0)
std  = top_scores.std(axis=0)
sharp = np.where(std>0, mean/std, -9)

best_idx = sharp.argmax()
best_w   = dict(zip(F_LIST, W[best_idx].round(4).tolist()))

print("=== BEST WEIGHT (sum=1) ===")
print(json.dumps(best_w, indent=2, ensure_ascii=False))
