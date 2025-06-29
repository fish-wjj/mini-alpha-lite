#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
向量化回测（每月调仓，ETF + α 组合）
* 用上一交易日因子打分选股，避免未来函数
* 一次性拉所有价格，提高速度
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from loguru import logger

from src.config import load_cfg
from src.utils import build_today_universe, latest_trade_date, prev_trade_date, safe_query, pro
from src.factor_model import score

plt.switch_backend("Agg")  # 无显示环境也能画图

CFG = load_cfg()
START = "20180102"  # 第一调仓日前一天
END = latest_trade_date()       # 最新一个可用价

REPORT_DIR = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


# ─────────────────── 交易日 & 调仓日 ───────────────────
trade_days = safe_query(
    pro.trade_cal, exchange="SSE", start_date=START, end_date=END
)
trade_days = pd.to_datetime(trade_days[trade_days.is_open == 1]["cal_date"])
rebal_dates = trade_days.groupby([trade_days.dt.year, trade_days.dt.month]).first().tolist()

# ─────────────────── 一次性取价格 ────────────────────
logger.info("下载 ETF 价格 …")
etf_px = safe_query(
    pro.fund_daily, ts_code=",".join([CFG["core_etf"], CFG["bond_etf"]]),
    start_date=START, end_date=END, fields="ts_code,trade_date,close"
).pivot(index="trade_date", columns="ts_code", values="close")

logger.info("回测 …")
equity = [1.0]
dates = []

all_stock_px: dict[str, pd.Series] = {}  # 动态累加已用股票价格

for i in tqdm(range(len(rebal_dates) - 1)):
    d0 = rebal_dates[i]          # 调仓日（当月首个交易日）
    d1 = rebal_dates[i + 1]      # 下一个调仓日
    prev_d0 = prev_trade_date(d0.strftime("%Y%m%d"))

    # 1) 上月末数据 → 选股
    uni = build_today_universe(prev_d0)
    alpha_codes = score(uni).head(CFG["num_alpha"])["ts_code"].tolist()

    # 2) 补全股票价格
    need = [c for c in alpha_codes if c not in all_stock_px]
    if need:
        for c in tqdm(need, desc="Fetch Stock", leave=False):
            px = safe_query(
                pro.daily,
                ts_code=c,
                start_date=d0.strftime("%Y%m%d"),
                end_date=d1.strftime("%Y%m%d"),
                fields="trade_date,close",
            ).set_index("trade_date")["close"]
            all_stock_px[c] = px
    # 3) 当期收益
date0_str = d0.strftime("%Y%m%d")
    date1_str = d1.strftime("%Y%m%d")

    # --- 计算 ETF 收益（增加健壮性检查）---
    r_etf = 0.0
    if CFG["core_etf"] in etf_px.columns and date0_str in etf_px.index and date1_str in etf_px.index:
        px0 = etf_px.loc[date0_str, CFG["core_etf"]]
        px1 = etf_px.loc[date1_str, CFG["core_etf"]]
        if pd.notna(px0) and pd.notna(px1) and px0 > 0:
            r_etf = (px1 / px0) - 1
    else:
        logger.warning(f"核心ETF {CFG['core_etf']} 在 {date0_str} 或 {date1_str} 缺少价格，当期收益计为 0")

    r_bond = 0.0
    if CFG["bond_etf"] in etf_px.columns and date0_str in etf_px.index and date1_str in etf_px.index:
        px0 = etf_px.loc[date0_str, CFG["bond_etf"]]
        px1 = etf_px.loc[date1_str, CFG["bond_etf"]]
        if pd.notna(px0) and pd.notna(px1) and px0 > 0:
            r_bond = (px1 / px0) - 1
    else:
        logger.warning(f"债券ETF {CFG['bond_etf']} 在 {date0_str} 或 {date1_str} 缺少价格，当期收益计为 0")

    # --- 计算 Alpha 股票收益（增加健壮性检查）---
    r_alpha = []
    for c in alpha_codes:
        px_series = all_stock_px.get(c)
        if px_series is not None and date0_str in px_series.index and date1_str in px_series.index:
            px0 = px_series.loc[date0_str]
            px1 = px_series.loc[date1_str]
            if pd.notna(px0) and pd.notna(px1) and px0 > 0:
                r_alpha.append(px1 / px0 - 1)
        else:
            logger.warning(f"Alpha股票 {c} 在 {date0_str} 或 {date1_str} 缺少价格，该股当期收益不计入")
    
    r_alpha_mean = np.mean(r_alpha) if r_alpha else 0

    # --- 合并计算总收益 ---
    ret = (CFG["core_ratio"] * r_etf +
           CFG["bond_ratio"] * r_bond +
           CFG["alpha_ratio"] * r_alpha_mean)

    equity.append(equity[-1] * (1 + ret))
    dates.append(d1)

# ─────────────────── 结果输出 ─────────────────────────
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["cummax"] = rep.equity.cummax()
rep["drawdown"] = rep.equity / rep.cummax - 1
rep["ret"] = rep.equity.pct_change().fillna(0)
rep.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

plt.figure(figsize=(9, 4))
plt.plot(rep.date, rep.equity)
plt.title("Equity Curve (2018-Now)")
plt.tight_layout()
plt.savefig(REPORT_DIR / "equity_curve.png")
logger.success(f"回测完成 → reports/backtest_report.csv & equity_curve.png")
