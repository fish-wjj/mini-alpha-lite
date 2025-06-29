# -*- coding: utf-8 -*-
"""
月度回测（一次性取价 + 向量化）
"""
from pathlib import Path
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from tqdm import tqdm

from src.config import load_cfg
from src.factor_model import score
from src.utils import get_today_universe, latest_trade_date, q, pro

cfg   = load_cfg()
START = "20180101"
END   = latest_trade_date()
REPORT = Path(__file__).resolve().parent.parent / "reports"
REPORT.mkdir(exist_ok=True)

# ---------- 1. 月度调仓日 ----------
trade_days = q(pro.trade_cal, exchange="SSE", start_date=START, end_date=END)
open_days  = pd.to_datetime(trade_days.loc[trade_days.is_open == 1, "cal_date"])
rebalance_dates = open_days.groupby([open_days.dt.year, open_days.dt.month]).first()

# ---------- 2. 价格下载 ----------
def fetch_prices(codes, start, end, is_fund=False):
    api = pro.fund_daily if is_fund else pro.daily
    dfs = []
    for c in tqdm(codes, desc=f"Fetch {'Fund' if is_fund else 'Stock'}"):
        df = q(api, ts_code=c, start_date=start, end_date=end,
               fields="ts_code,trade_date,close")
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    full = pd.concat(dfs, ignore_index=True)
    return (full.pivot(index="trade_date", columns="ts_code", values="close")
                .sort_index())

etf_codes  = [cfg["core_etf"], cfg["bond_etf"]]
price_etf  = fetch_prices(etf_codes, START, END, is_fund=True)
price_all  = price_etf.copy()

# ---------- 3. 回测 ----------
equity = [1.0]
dates  = []

for i in tqdm(range(len(rebalance_dates) - 1), desc="Backtesting"):
    d0 = rebalance_dates.iloc[i].strftime("%Y%m%d")
    d1 = rebalance_dates.iloc[i + 1].strftime("%Y%m%d")

    # —— 截面选股
    uni  = get_today_universe()
    top  = score(uni).head(cfg["num_alpha"])
    alpha_codes = top["ts_code"].tolist()

    # —— 若有新股票价格缺失，就补下载
    new_codes = [c for c in alpha_codes if c not in price_all.columns]
    if new_codes:
        price_all = price_all.reindex(columns=price_all.columns.union(new_codes))
        px_new    = fetch_prices(new_codes, d0, END)
        price_all.loc[px_new.index, new_codes] = px_new

    # —— 本周期首尾价格
    px_period = price_all.loc[d0:d1]
    if px_period.empty:
        continue
    px0, px1 = px_period.iloc[0], px_period.iloc[-1]

    def _ret(cds, ratio):
        if not cds:
            return 0.0
        valid = [c for c in cds if pd.notna(px0.get(c)) and pd.notna(px1.get(c))]
        if not valid:
            return 0.0
        return ratio * ((px1[valid] / px0[valid] - 1).mean())

    r_alpha = _ret(alpha_codes,        cfg["alpha_ratio"])
    r_core  = _ret([cfg["core_etf"]],  cfg["core_ratio"])
    r_bond  = _ret([cfg["bond_etf"]],  cfg["bond_ratio"])

    equity.append(equity[-1] * (1 + r_alpha + r_core + r_bond))
    dates.append(pd.to_datetime(d1))

# ---------- 4. 结果 ----------
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["ret"]      = rep["equity"].pct_change().fillna(0)
rep["cummax"]   = rep["equity"].cummax()          # ← 关键：加 ()
rep["drawdown"] = rep["equity"] / rep["cummax"] - 1
rep.to_csv(REPORT / "backtest_report.csv", index=False)

plt.figure(figsize=(10, 4))
plt.plot(rep["date"], rep["equity"])
plt.title("Equity Curve 2018-Now (Fast)")
plt.tight_layout()
plt.savefig(REPORT / "equity_curve.png")

print("✓ 回测完成 → reports/backtest_report.csv & equity_curve.png")
