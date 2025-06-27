# -*- coding: utf-8 -*-
"""
月度调仓回测 2018-01-01 → 最新交易日
一次性拉 40 日全市场行情，降低 API 调用至月均 2 次
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

# ---------- 参数 ---------------------------------------------------------- #
START_DATE  = "20180101"
CORE_ETF    = "159949.SZ"
CORE_WGT    = 0.60
NUM_ALPHA   = 5
ALPHA_WGT   = (1 - CORE_WGT) / NUM_ALPHA
REPORT_DIR  = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True, parents=True)

TS_TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TS_TOKEN)
# ------------------------------------------------------------------------- #

# 1) 交易日序列
cal = safe_query(pro.trade_cal, exchange="SSE",
                 start_date=START_DATE, end_date=latest_trade_date())
trade_days = pd.to_datetime(cal[cal["is_open"] == 1]["cal_date"])
month_starts = trade_days[trade_days.dt.day == 1]

equity, dates = [1.0], []

logger.info(f"回测区间：{month_starts.iloc[0].date()} → {month_starts.iloc[-1].date()}")

for day in tqdm(month_starts, desc="Backtest"):
    trd = day.strftime("%Y%m%d")
    start40 = (day - pd.Timedelta(days=40)).strftime("%Y%m%d")

    # --- 拉全市场近 40 日行情一次搞定 ---
    hist = safe_query(
        pro.daily, start_date=start40, end_date=trd,
        fields="ts_code,trade_date,close,pct_chg,amount"
    )
    hist["trade_date"] = pd.to_datetime(hist["trade_date"])
    last = hist[hist["trade_date"] == pd.to_datetime(trd)]
    last = last[["ts_code", "close", "amount"]]

    # PE/PB
    basic = safe_query(
        pro.daily_basic, trade_date=trd,
        fields="ts_code,pe_ttm,pb"
    )

    # 20 日动量向量化
    mom = (hist.set_index("trade_date")
                 .groupby("ts_code")["pct_chg"]
                 .rolling(20).sum()
                 .reset_index())
    mom = mom[mom["trade_date"] == pd.to_datetime(trd)][["ts_code","pct_chg"]]
    mom.rename(columns={"pct_chg":"pct_chg_20d"}, inplace=True)

    universe = last.merge(basic, on="ts_code").merge(mom, on="ts_code")
    ranked = score(universe).head(NUM_ALPHA)
    alpha_codes = ranked["ts_code"].tolist()

    # 月末日期
    end_day = (day + pd.offsets.MonthEnd()).strftime("%Y%m%d")

    def px(c, d): return float(
        safe_query(pro.daily, ts_code=c, trade_date=d, fields="close")["close"].iloc[0])

    r_etf = (px(CORE_ETF, end_day) - px(CORE_ETF, trd)) / px(CORE_ETF, trd)
    r_stock = np.mean([(px(c, end_day) - px(c, trd)) / px(c, trd) for c in alpha_codes])

    r_port = CORE_WGT * r_etf + (1 - CORE_WGT) * r_stock
    equity.append(equity[-1] * (1 + r_port))
    dates.append(pd.to_datetime(end_day))

# ----------- 报表 & 图 ---------------------------------------------------- #
rep = pd.DataFrame({"date": dates, "equity": equity[1:]})
rep["ret"] = rep["equity"].pct_change().fillna(0)
rep["cummax"] = rep["equity"].cummax()
rep["drawdown"] = rep["equity"] / rep["cummax"] - 1
rep.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

plt.figure(figsize=(10,4))
plt.plot(rep["date"], rep["equity"])
plt.title("Equity Curve 2018-Now")
plt.tight_layout()
plt.savefig(REPORT_DIR / "equity_curve.png")
logger.success("回测完成 → reports/backtest_report.csv & equity_curve.png")
