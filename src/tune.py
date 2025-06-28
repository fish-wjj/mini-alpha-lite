# -*- coding: utf-8 -*-
"""
网格搜索 6 因子权重
• 先生成 6 维权重向量 w_raw
• 再归一化 w = w_raw / sum(w_raw)  → 保证权重和 = 1
• 目标函数: Top-50 得分夏普比
"""
import itertools, json, numpy as np
from src.utils import get_today_universe
from src.factor_model import score, F_LIST

GRID = [0, 0.05, 0.10, 0.15, 0.20, 0.25]   # 6^6 = 46 656 组合，仍秒级
TOP_N = 50

df_base = get_today_universe()              # 只抓一次

def sharp(s):                               # 夏普
    return s.mean() / s.std(ddof=0) if s.std(ddof=0) else -9

best_w, best_s = None, -9

for comb in itertools.product(GRID, repeat=len(F_LIST)):
    if not any(comb):        # 全 0 跳过
        continue
    w = dict(zip(F_LIST, comb))
    total = sum(w.values())
    w = {k: v / total for k, v in w.items()}  # 归一

    Top = score(df_base.copy(), w).head(TOP_N)["score"]
    s   = sharp(Top)
    if s > best_s:
        best_s, best_w = s, w

print("=== BEST WEIGHT (sum=1) ===")
print(json.dumps(best_w, indent=2, ensure_ascii=False))
