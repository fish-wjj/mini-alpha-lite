# -*- coding: utf-8 -*-
import datetime as dt, numpy as np, pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm

from src.config import load_cfg
from src.factor_model import score
from src.utils import get_today_universe, latest_trade_date, q, pro, CACHE

cfg = load_cfg()
REPORT = Path(__file__).resolve().parent.parent / "reports"
REPORT.mkdir(exist_ok=True)

START = "20180101"
END   = latest_trade_date()

# ---------- 1. 月度调仓日期 ----------
trade_days = q(pro.trade_cal, exchange="SSE", start_date=START, end_date=END)
open_days  = pd.to_datetime(trade_days[trade_days.is_open == 1]["cal_date"])
rebalance_dates = open_days.to_series().groupby([open_days.dt.year, open_days.dt.month]).first()

# ---------- 2. 预下载所有价格 ----------
def fetch_prices(codes, start, end, is_fund=False):
    api = pro.fund_daily if is_fund else pro.daily
    dfs = []
    for c in tqdm(codes, desc=f"Fetch {'Fund' if is_fund else 'Stock'}"):
        df = q(api, ts_code=c, start_date=start, end_date=end, fields="ts_code,trade_date,close")
        dfs.append(df)
    full = pd.concat(dfs)
    return full.pivot(index="trade_date", columns="ts_code", values="close").sort_index()

print("◎ 下载 ETF 价格 …")
etf_codes = [cfg["core_etf"], cfg["bond_etf"]]
price_etf = fetch_prices(etf_codes, START, END, is_fund=True)

# 先留空 DataFrame，后面调仓日动态拼接个股价格
price_all = price_etf.copy()

# ---------- 3. 回测主循环 ----------
equity = [1.0]
dates  = []

print("◎ 回测 …")
for i in tqdm(range(len(rebalance_dates) - 1)):
    d0 = rebalance_dates.iloc[i].strftime("%Y%m%d")
    d1 = rebalance_dates.iloc[i + 1].strftime("%Y%m%d")

    # 截面选股
    uni = get_today_universe()
    top = score(uni).head(cfg["num_alpha"])
    alpha_codes = top["ts_code"].tolist()

    # 若有新股票，追加价格列
    new_codes = [c for c in alpha_codes if c not in price_all.columns]
    if new_codes:
        price_new = fetch_prices(new_codes, d0, END)
        price_all = price_all.reindex(columns=price_all.columns.union(price_new.columns))
        price_all.loc[price_new.index, new_codes] = price_new

    # 取周期首尾价格
    px_period = price_all.loc[d0:d1]
    if px_period.empty:
        continue
    px_start, px_end = px_period.iloc[0], px_period.iloc[-1]

    def _ret(codes, ratio, lot=1):
        if not codes:
            return 0
        valid = [c for c in codes if pd.notna(px_start.get(c)) and pd.notna(px_end.get(c))]
        if not valid:
            return 0
        return ratio * ((px_end[valid] / px_start[valid] - 1).mean())

    r_alpha = _ret(alpha_codes, cfg["alpha_ratio"])
    r_core  = _ret([cfg["core_etf"]], cfg["core_ratio"])
    r_bond  = _ret([cfg["bond_etf"]], cfg["bond_ratio"])

    equity.append(equity[-1] * (1 + r_alpha + r_core + r_bond))
    dates.append(pd.to_datetime(d1))

# ---------- 4. 生成报告 ----------
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["ret"] = rep.equity.pct_change().fillna(0)
rep["cummax"] = rep.equity.cummax()
rep["drawdown"] = rep.equity / rep.cummax - 1
rep.to_csv(REPORT / "backtest_report.csv", index=False)

plt.figure(figsize=(10, 4))
plt.plot(rep.date, rep.equity)
plt.title("Equity Curve 2018-Now (Fast)")
plt.tight_layout()
plt.savefig(REPORT / "equity_curve.png")

print("✓ 回测完成 → reports/backtest_report.csv & equity_curve.png")
