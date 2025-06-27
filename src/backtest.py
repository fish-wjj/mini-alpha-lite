# -*- coding: utf-8 -*-
"""
月度调仓向前回测
周期: 2018-01-01 → 最近交易日
规则:
  • 月首交易日收盘后, 计算三因子得分, 选前 5 只股票 + 60% 红利 ETF
  • 资金权重: ETF 60%, 每只股票 8% (如不足 5 只则等权)
  • 不考虑手续费、分红, 仅看价差收益
输出:
  reports/backtest_report.csv
  reports/equity_curve.png
"""
import os, datetime as dt, pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
from src.utils import safe_query, latest_trade_date
from src.factor_model import score
from src.logger import logger

# ——— 参数 ————————————————————————————————————————————— #
START = "20180101"
CORE_ETF = "159949.SZ"
CORE_WEIGHT = 0.60
NUM_ALPHA = 5
ALPHA_WEIGHT = (1 - CORE_WEIGHT) / NUM_ALPHA
REPORT_DIR = Path(__file__).resolve().parent.parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)
# —————————————————————————————————————————————————————— #

def get_trade_calendar() -> pd.DatetimeIndex:
    end = latest_trade_date()
    cal = safe_query(
        pro.trade_cal, exchange="SSE", start_date=START, end_date=end
    )
    cal = cal[cal["is_open"] == 1]["cal_date"]
    return pd.to_datetime(cal)

def fetch_daily(ts_code: str, start: str, end: str) -> pd.Series:
    df = safe_query(pro.daily, ts_code=ts_code, start_date=start, end_date=end)
    df = df[["trade_date", "close"]]
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date")["close"].sort_index()

def build_universe(trade_date: str) -> pd.DataFrame:
    # 拉日行情 + basic 因子
    daily = safe_query(pro.daily, trade_date=trade_date)[
        ["ts_code", "close", "pct_chg", "amount"]
    ]
    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb",
    )
    raw = daily.merge(basic, on="ts_code", how="left").fillna(0)
    return score(raw)

if __name__ == "__main__":
    import tushare as ts, os
    pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

    trade_days = get_trade_calendar()
    trade_days = trade_days[pd.to_datetime(trade_days).dt.day == 1]
    equity = [1.0]  # 初始净值
    dates = []

    for dt_day in tqdm(trade_days, desc="回测"):
        dt_str = dt_day.strftime("%Y%m%d")
        ranked = build_universe(dt_str)
        alpha = ranked.head(NUM_ALPHA)["ts_code"].tolist()

        # 拉本月收益
        month_end = (dt_day + pd.offsets.MonthEnd(1)).strftime("%Y%m%d")
        price_start = {c: fetch_daily(c, dt_str, dt_str).iloc[-1] for c in alpha}
        price_end = {c: fetch_daily(c, month_end, month_end).iloc[-1] for c in alpha}

        etf_start = fetch_daily(CORE_ETF, dt_str, dt_str).iloc[-1]
        etf_end = fetch_daily(CORE_ETF, month_end, month_end).iloc[-1]

        # 计算组合月收益
        r_etf = (etf_end - etf_start) / etf_start
        r_stock = np.mean(
            [(price_end[c] - price_start[c]) / price_start[c] for c in alpha]
        )
        r_port = CORE_WEIGHT * r_etf + (1 - CORE_WEIGHT) * r_stock

        equity.append(equity[-1] * (1 + r_port))
        dates.append(month_end)

    # 结果 DataFrame
    report = pd.DataFrame({"date": dates, "equity": equity[1:]})
    report["ret"] = report["equity"].pct_change().fillna(0)
    report["cum_max"] = report["equity"].cummax()
    report["drawdown"] = report["equity"] / report["cum_max"] - 1
    report.to_csv(REPORT_DIR / "backtest_report.csv", index=False)

    # 画曲线
    plt.figure(figsize=(10, 4))
    plt.plot(report["date"], report["equity"])
    plt.title("Equity Curve 2018-Now")
    plt.tight_layout()
    plt.savefig(REPORT_DIR / "equity_curve.png")
    logger.success("回测完成 → reports/backtest_report.csv & equity_curve.png")
