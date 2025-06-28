# -*- coding: utf-8 -*-
"""
月度调仓回测（2018-01-01 → 最新交易日）
• 一次性拉 40 日全市场行情（防止 TuShare 限流）
• 若 ETF/股票在当月无行情 ⇒ 该部分收益计 0，不中断
输出：
  reports/backtest_report.csv   — 每月净值 / 收益 / 回撤
  reports/equity_curve.png      — 净值曲线
依赖：pandas numpy matplotlib tqdm tushare
"""
import os, datetime as dt, pandas as pd, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
import tushare as ts
from src.utils import safe_query, latest_trade_date
from src.factor_model import score
from src.logger import logger

# ─── 参数（如改 ETF，只需改这两个常量） ────────────────────────────── #
START_DATE  = "20180101"
CORE_ETF    = "510880.SH"    # 红利 ETF，2012 上市（历史更长）
CORE_WGT    = 0.60
NUM_ALPHA   = 5
ALPHA_WGT   = (1 - CORE_WGT) / NUM_ALPHA
REPORT_DIR  = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
# ─────────────────────────────────────────────────────────────────── #

pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
LATEST = pd.to_datetime(latest_trade_date())


# ---------- 通用工具 ------------------------------------------------ #
def get_trade_days() -> pd.Series:
    cal = safe_query(
        pro.trade_cal,
        exchange="SSE",
        start_date=START_DATE,
        end_date=LATEST.strftime("%Y%m%d"),
    )
    return pd.to_datetime(cal[cal["is_open"] == 1]["cal_date"]).sort_values()


def get_last_close(ts_code: str, date: str) -> float | None:
    """返回 ≤date 最近一个交易日的收盘价；ETF 用 fund_daily"""
    is_etf = ts_code.startswith(("15", "51", "56"))
    api    = pro.fund_daily if is_etf else pro.daily
    df = safe_query(api, ts_code=ts_code, end_date=date, limit=1, fields="close")
    if df.empty:
        return None
    return float(df["close"].iloc[0])


# ---------- 主循环 -------------------------------------------------- #
trade_days   = get_trade_days()
month_starts = trade_days[trade_days.dt.day == 1]          # 升序
logger.info(f"回测区间：{month_starts.iloc[0].date()} → {month_starts.iloc[-1].date()}")

equity, dates = [1.0], []

for day in tqdm(month_starts, desc="Backtest"):
    trd = day.strftime("%Y%m%d")
    start40 = (day - pd.Timedelta(days=40)).strftime("%Y%m%d")

    # 全市场近 40 日行情（一次 API 调用）
    hist = safe_query(
        pro.daily,
        start_date=start40,
        end_date=trd,
        fields="ts_code,trade_date,close,pct_chg,amount",
    )
    hist["trade_date"] = pd.to_datetime(hist["trade_date"])
    last = hist[hist["trade_date"] == day][["ts_code", "close", "amount"]]

    basic = safe_query(
        pro.daily_basic, trade_date=trd, fields="ts_code,pe_ttm,pb"
    )

    # 20 日动量（向量化）
    mom = (
        hist.set_index("trade_date")
        .groupby("ts_code")["pct_chg"]
        .rolling(20)
        .sum()
        .reset_index()
    )
    mom = mom[mom["trade_date"] == day][["ts_code", "pct_chg"]]
    mom.rename(columns={"pct_chg": "pct_chg_20d"}, inplace=True)

    universe = last.merge(basic, on="ts_code").merge(mom, on="ts_code")
    ranked   = score(universe).head(NUM_ALPHA)
    alpha    = ranked["ts_code"].tolist()

    # 月末日期；如超出最新交易日则结束
    end_day_dt = day + pd.offsets.MonthEnd()
    if end_day_dt > LATEST:
        break
    end_day = end_day_dt.strftime("%Y%m%d")

    # —— ETF 部分收益 —— #
    px_s = get_last_close(CORE_ETF, trd)
    px_e = get_last_close(CORE_ETF, end_day)
    r_etf = 0 if (px_s is None or px_e is None) else (px_e - px_s) / px_s

    # —— α 仓收益 —— #
    r_vals = []
    for c in alpha:
        s = get_last_close(c, trd)
        e = get_last_close(c, end_day)
        if s is not None and e is not None:
            r_vals.append((e - s) / s)
    r_stock = np.mean(r_vals) if r_vals else 0

    # —— 组合净值 —— #
    equity.append(equity[-1] * (1 + CORE_WGT * r_etf + (1 - CORE_WGT) * r_stock))
    dates.append(end_day_dt)

# ---------- 报表 & 图 ------------------------------------------------ #
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["ret"] = rep["equity"].pct_change().fillna(0)
rep["cummax"] = rep["equity"].cummax()
rep["drawdown"] = rep["equity"] / rep["cummax"] - 1
rep.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

plt.figure(figsize=(10, 4))
plt.plot(rep["date"], rep["equity"])
plt.title("Equity Curve 2018-Now")
plt.tight_layout()
plt.savefig(REPORT_DIR / "equity_curve.png")
logger.success("回测完成 → reports/backtest_report.csv & equity_curve.png")
