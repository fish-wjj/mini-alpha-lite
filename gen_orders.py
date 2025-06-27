# -*- coding: utf-8 -*-
"""
生成批量下单 CSV
• 核心 ETF: 60 %
• α 仓:     5 只 × 6 %（自动跳过买不起 1 手的高价股）
CSV 采用 UTF-8-SIG 编码
"""
import datetime as dt, pandas as pd
from utils import pro, get_today_universe
from factor_model import score

# ----------- 全局参数 -----------
TOTAL_CASH  = 50_000      # 总本金
CORE_RATIO  = 0.60        # 核心 ETF 权重
ALPHA_RATIO = 0.30        # α 仓权重
NUM_ALPHA   = 5           # 目标 α 仓数量
CORE_ETF    = "159949"    # 央企红利 ETF，无后缀
LOT_SIZE    = 100         # A 股最小 1 手 = 100 股
# --------------------------------

today      = dt.date.today()
trade_date = today.strftime("%Y%m%d")
print("• 获取今日行情 & 因子…")
df = get_today_universe()                     # 不再过滤成交额

orders = []

# ===== 1) 核心 ETF ==========================================================
etf_code = CORE_ETF + ".SZ"
etf_data = pro.fund_daily(ts_code=etf_code, end_date=trade_date, limit=1)
if etf_data.empty:
    raise RuntimeError(f"无法获取 {CORE_ETF} 行情")
core_price = float(etf_data.iloc[0]["close"])
core_qty   = int((TOTAL_CASH * CORE_RATIO) // (core_price * LOT_SIZE)) * LOT_SIZE
orders.append([CORE_ETF, "B", 0, core_qty])   # 市价委托

# ===== 2) α 仓选股 =========================================================
alpha_pool  = score(df).reset_index(drop=True)   # 按打分降序
alpha_list  = []
cursor      = 0
max_cursor  = len(alpha_pool)
cash_each   = TOTAL_CASH * ALPHA_RATIO / NUM_ALPHA

while len(alpha_list) < NUM_ALPHA and cursor < max_cursor:
    row      = alpha_pool.loc[cursor]
    ts_code  = row["ts_code"]
    code6    = ts_code.split(".")[0]
    if code6 == CORE_ETF:
        cursor += 1
        continue                                # 避免与核心 ETF 重复
    price = round(row["close"] * 1.01, 2)       # +1 % 滑点保护
    qty   = int(cash_each // (price * LOT_SIZE)) * LOT_SIZE
    if qty >= LOT_SIZE:                         # 能买一手才加入
        alpha_list.append((code6, price, qty))
    cursor += 1

# 若实在凑不够 5 只，就把现金均分到已有标的再重算数量
if 0 < len(alpha_list) < NUM_ALPHA:
    cash_each = TOTAL_CASH * ALPHA_RATIO / len(alpha_list)
    alpha_list = [
        (code, price, int(cash_each // (price * LOT_SIZE)) * LOT_SIZE)
        for code, price, _ in alpha_list
    ]

# ===== 3) 写入订单行 ========================================================
for code, price, qty in alpha_list:
    orders.append([code, "B", price, qty])

# ===== 4) 保存 CSV (UTF-8-SIG) =============================================
csv = pd.DataFrame(orders,
                   columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn  = f"orders_{trade_date}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
print(f"✅  已生成批量下单文件: {fn}")
