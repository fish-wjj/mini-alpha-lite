# -*- coding: utf-8 -*-
"""
生成批量下单 CSV（含债券 ETF 10 份/手处理 + 趋势过滤 + 新权重）
"""
import json, datetime as dt, pathlib, pandas as pd
from src.config import load_cfg; cfg = load_cfg()
from src.utils import get_today_universe, pro, is_risk_off
from src.factor_model import score
from src.logger import logger

BASE   = pathlib.Path(__file__).resolve().parent.parent
CSV_DIR= BASE / "orders"; CSV_DIR.mkdir(exist_ok=True)
STATE_FP = BASE / "state_portfolio.json"

td = dt.date.today().strftime("%Y%m%d")
df = get_today_universe()
risk = is_risk_off(td)

# ——— 股票池过滤 ———
stock = df[(~df["ts_code"].str.startswith(("15","51","56"))) &
           (df["amount"] >= cfg["min_amount"])]
if stock.empty: stock = df[~df["ts_code"].str.startswith(("15","51","56"))]
etfs  = df[df["ts_code"].str.startswith(("15","51","56"))]
df_filt = pd.concat([stock, etfs])

alpha_list = []
if not risk:
    alpha_list = score(df_filt).head(cfg["num_alpha"])["ts_code"].tolist()
else:
    logger.warning("大盘跌破 200MA，仅持 ETF")

orders=[]
LOT_STK = cfg["lot"]
LOT_BOND= 10   # 债券 ETF 1 手 = 10 份

# ——— 核心指数 ETF ———
core_px = pro.fund_daily(ts_code=cfg["core_etf"], limit=1)["close"].iat[0]
core_qty= int(cfg["cash"]*cfg["core_ratio"] // (core_px*LOT_STK)) * LOT_STK
orders.append([cfg["core_etf"].split(".")[0], "B", 0, core_qty])

# ——— 债券 ETF ———
bond_px = pro.fund_daily(ts_code=cfg["bond_etf"], limit=1)["close"].iat[0]
bond_qty= int(cfg["cash"]*cfg["bond_ratio"] // (bond_px*LOT_BOND)) * LOT_BOND
orders.append([cfg["bond_etf"].split(".")[0], "B", 0, bond_qty])

# ——— α 仓买入 ———
alpha_cash_each = cfg["cash"]*cfg["alpha_ratio"] / max(len(alpha_list), 1)
for code in alpha_list:
    price = df.loc[df["ts_code"] == code, "close"].iat[0] * 1.01
    qty   = int(alpha_cash_each // (price*LOT_STK)) * LOT_STK
    if qty >= LOT_STK:
        orders.append([code.split(".")[0], "B", round(price,2), qty])

# ——— 输出 CSV ———
csv_fp = CSV_DIR / f"orders_{td}.csv"
pd.DataFrame(orders, columns=["证券代码","买卖标志","委托价格","委托数量"])\
  .to_csv(csv_fp, index=False, encoding="utf-8-sig")
logger.success(f"下单文件已生成 → {csv_fp}")
