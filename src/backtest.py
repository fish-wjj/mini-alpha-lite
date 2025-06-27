# -*- coding: utf-8 -*-
"""
月度调仓回测 2018-01-01 → 最新交易日
输出：
  reports/backtest_report.csv      每月净值、收益、回撤
  reports/equity_curve.png         净值曲线
需要：
  pandas / numpy / matplotlib / tushare / tqdm
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

# ─── 参数 ─────────────────────────────────────────────────────────── #
START_DATE  = "20180101"
CORE_ETF    = "159949.SZ"
CORE_WGT    = 0.60
NUM_ALPHA   = 5
ALPHA_WGT   = (1 - CORE_WGT) / NUM_ALPHA
REPORT_DIR  = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True, parents=True)
# ─────────────────────────────────────────────────────────────────── #

TS_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TS_TOKEN)

# ---------- 辅助函数 ------------------------------------------------ #
def trade_calendar() -> pd.DatetimeIndex:
    end = latest_trade_date()
    cal = safe_query(pro.trade_cal, exchange="SSE",
                     start_date=START_DATE, end_date=end)
    open_days = cal[cal["is_open"] == 1]["cal_date"]
    return pd.to_datetime(open_days)

def momentum_20d(ts_code: str, start: str, end: str) -> pd.Series:
    df = safe_query(pro.daily, ts_code=ts_code,
                    start_date=start, end_date=end,
                    fields="ts_code,trade_date,pct_chg")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return (df.set_index("trade_date")["pct_chg"]
              .rolling(20).sum().dropna())

def fetch_close(ts_code: str, trade_date: str) -> float:
    px = safe_query(pro.daily, ts_code=ts_code,
                    trade_date=trade_date, fields="close")
    return float(px["close"].iloc[0])

def build_universe(trd: str) -> pd.DataFrame:
    daily  = safe_query(pro.daily, trade_date=trd)[
        ["ts_code", "close", "amount"]]
    basic  = safe_query(pro.daily_basic, trade_date=trd,
                        fields="ts_code,pe_ttm,pb")
    raw = daily.merge(basic, on="ts_code", how="left").fillna(0)

    # 追加 20 日动量
    momo_list = []
    for code in raw["ts_code"]:
        mom = momentum_20d(code, 
               (pd.to_datetime(trd)-pd.Timedelta(days=40)).strftime("%Y%m%d"),
               trd)
        momo_list.append(mom.iloc[-1] if not mom.empty else 0)
    raw["pct_chg_20d"] = momo_list
    return score(raw)

# ---------- 回测主循环 --------------------------------------------- #
trade_days = trade_calendar()
month_starts = trade_days[trade_days.day == 1]

equity = [1.0]
dates  = []

logger.info(f"开始回测：{month_starts[0].date()} → {month_starts[-1].date()}")

for day in tqdm(month_starts, desc="回测"):
    trd = day.strftime("%Y%m%d")
    ranked = build_universe(trd)
    alpha  = ranked.head(NUM_ALPHA)["ts_code"].tolist()

    # 本月末
    end_day = (day + pd.offsets.MonthEnd()).strftime("%Y%m%d")

    # ETF 月收益
    etf_start = fetch_close(CORE_ETF, trd)
    etf_end   = fetch_close(CORE_ETF, end_day)
    r_etf = (etf_end - etf_start) / etf_start

    # α 月收益
    r_stock = np.mean([
        (fetch_close(c, end_day) - fetch_close(c, trd)) / fetch_close(c, trd)
        for c in alpha
    ])

    r_port = CORE_WGT * r_etf + (1 - CORE_WGT) * r_stock
    equity.append(equity[-1] * (1 + r_port))
    dates.append(pd.to_datetime(end_day))

# ---------- 报表 & 图 ------------------------------------------------ #
report = pd.DataFrame({"date": dates, "equity": equity[1:]})
report["ret"] = report["equity"].pct_change().fillna(0)
report["cummax"] = report["equity"].cummax()
report["drawdown"] = report["equity"] / report["cummax"] - 1
report.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

plt.figure(figsize=(10,4))
plt.plot(report["date"], report["equity"])
plt.title("Equity Curve 2018-Now")
plt.tight_layout()
plt.savefig(REPORT_DIR / "equity_curve.png")
logger.success("回测完成 → reports/backtest_report.csv & equity_curve.png")
