# -*- coding: utf-8 -*-
"""
月度回测（无前视）：
* 调仓信号使用“前一交易日”截面
* 一次性获取全市场价格，加速循环
"""
from __future__ import annotations
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from tqdm import tqdm
from loguru import logger

from src.utils import (trade_cal, latest_trade_date,
                       get_universe_on, pro)
from src.factor_model import score
from src.config import load_cfg
cfg = load_cfg()

START = "20180201"
END   = latest_trade_date()
REPORT_DIR = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)

# ── 1. 交易日 & 调仓日 ────────────────────────────────────────────────
cal = trade_cal(exchange='SSE', start_date=START, end_date=END)
open_days = pd.to_datetime(cal[cal["is_open"] == 1]["cal_date"]).sort_values()
rebalance_dates = open_days.to_series().groupby([open_days.dt.year,
                                                 open_days.dt.month]).first()

# ── 2. 一次性下载价格 ─────────────────────────────────────────────────
logger.info("下载全市场日线 …")
all_codes = pro.stock_basic(exchange='', list_status='L', fields='ts_code')['ts_code'].tolist()
etf_codes = [cfg["core_etf"], cfg["bond_etf"]]
price_frames = []
for code in tqdm(all_codes + etf_codes, desc="Fetch Price"):
    df = pro.daily(ts_code=code, start_date=START, end_date=END, fields="ts_code,trade_date,close")
    price_frames.append(df)
price_raw = pd.concat(price_frames)
price_mat = (price_raw.pivot(index='trade_date', columns='ts_code', values='close')
                        .sort_index()
                        .ffill())

# ── 3. 回测主循环 ────────────────────────────────────────────────────
equity = [1.0]
dates  = []

for i in tqdm(range(1, len(rebalance_dates)), desc="回测"):
    signal_day = rebalance_dates.iloc[i-1]   # 用前一交易日做选股
    exec_day   = rebalance_dates.iloc[i]     # 当月首日执行
    signal_str = signal_day.strftime("%Y%m%d")
    exec_str   = exec_day.strftime("%Y%m%d")

    uni = get_universe_on(signal_str)
    ranked = score(uni).head(cfg["num_alpha"])
    alpha = ranked["ts_code"].tolist()

    # 本周期收益
    if exec_str not in price_mat.index:
        continue  # 极端情况：无价格
    start_px = price_mat.loc[exec_str]
    # 取到下个 rebalance_end（含当月最后一天）
    end_px = price_mat.loc[rebalance_dates.iloc[i+1].strftime("%Y%m%d")] if i+1 < len(rebalance_dates) else price_mat.iloc[-1]

    r_alpha = ((end_px[alpha] / start_px[alpha]) - 1).mean() if alpha else 0
    r_core  = (end_px[cfg["core_etf"]] / start_px[cfg["core_etf"]] - 1)
    r_bond  = (end_px[cfg["bond_etf"]] / start_px[cfg["bond_etf"]] - 1)

    ret = (cfg["alpha_ratio"] * r_alpha +
           cfg["core_ratio"]  * r_core  +
           cfg["bond_ratio"]  * r_bond)
    equity.append(equity[-1] * (1 + ret))
    dates.append(exec_day)

# ── 4. 报告输出 ──────────────────────────────────────────────────────
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["ret"] = rep.equity.pct_change().fillna(0)
rep["cummax"] = rep.equity.cummax()
rep["drawdown"] = rep.equity / rep.cummax - 1
rep.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

plt.figure(figsize=(10,4))
plt.plot(rep.date, rep.equity)
plt.tight_layout()
plt.savefig(REPORT_DIR / "equity_curve.png")
logger.success("回测完成 → reports/backtest_report.csv & equity_curve.png")
