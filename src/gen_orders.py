# -*- coding: utf-8 -*-
"""
生成批量下单 CSV (UTF-8-SIG)
• 核心 ETF 60 %
• α 仓 5 只 × 6 %
  - 能买 ≥1 手(100 股) 即入选
"""
from src.logger import logger
import datetime as dt
import pandas as pd
from src.utils import pro, get_today_universe
from src.factor_model import score

TOTAL_CASH = 50_000
CORE_RATIO = 0.60
ALPHA_RATIO = 0.30
NUM_ALPHA = 5
CORE_ETF = "159949"
LOT = 100  # 最小手数

today = dt.date.today().strftime("%Y%m%d")
logger.info("拉取今日行情 & 因子…")
df = get_today_universe()  # 不过滤成交额

orders = []

# —— 核心 ETF —— #
etf_code = f"{CORE_ETF}.SZ"
etf_px = pro.fund_daily(ts_code=etf_code, limit=1)["close"].iat[0]
core_qty = int(TOTAL_CASH * CORE_RATIO // (etf_px * LOT)) * LOT
orders.append([CORE_ETF, "B", 0, core_qty])

# —— α 仓 —— #
alpha_cash = TOTAL_CASH * ALPHA_RATIO
candidates = score(df).reset_index(drop=True)
chosen = []
for _, row in candidates.iterrows():
    if len(chosen) == NUM_ALPHA:
        break
    code6 = row["ts_code"].split(".")[0]
    if code6 == CORE_ETF:
        continue
    px = round(row["close"] * 1.01, 2)
    qty = LOT  # 先买 1 手
    if px * qty <= alpha_cash / NUM_ALPHA:
        chosen.append((code6, px, qty))

# 如果仍不足 5 只，平均分现金再重算 qty
if 0 < len(chosen) < NUM_ALPHA:
    cash_each = alpha_cash / len(chosen)
    chosen = [(c, p, int(cash_each // (p * LOT)) * LOT) for c, p, _ in chosen]

orders += [[c, "B", p, q] for c, p, q in chosen]

# —— 写 CSV —— #
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn = f"orders_{today}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"已生成 CSV: {fn}")
