# -*- coding: utf-8 -*-
"""
utils.py
========
• latest_trade_date()          : 找最近交易日 (字符串 yyyymmdd)
• get_today_universe()         : 一次性返回 6 因子所需全部字段
    ├─ 日行情  (close / pct_chg / amount)
    ├─ daily_basic  (pe_ttm / pb / turnover_rate_f)
    ├─ ROA   (最近财报期 fina_indicator)
    ├─ 20 日动量   pct_chg_20d
    └─ 20 日波动率 vol_20d
"""
from dotenv import load_dotenv; load_dotenv()
import os, datetime as dt, pandas as pd, tushare as ts
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger

# ─── TuShare 客户端 ──────────────────────────────────────────────────────── #
pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def safe_query(fn, **kwargs):
    """对 TuShare 调用做重试"""
    return fn(**kwargs)

# ─── 获取最近交易日 ───────────────────────────────────────────────────────── #
def latest_trade_date() -> str:
    today = dt.date.today()
    for i in range(5):
        d  = today - dt.timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        cal = safe_query(pro.trade_cal, exchange="SSE", start_date=ds, end_date=ds)
        if cal.iloc[0]["is_open"] == 1:
            return ds
    raise RuntimeError("未找到最近交易日")

# ─── 财报期工具 (上一季末) ─────────────────────────────────────────────────── #
def _last_quarter(date: str) -> str:
    y, m = int(date[:4]), int(date[4:6])
    q = (m - 1) // 3  # 0–3
    if q == 0:                   # 1月-3月 → 去年 Q4
        y -= 1; q = 4
    return f"{y}{q*3:02d}31"     # 20240331 / 20231231 …

# ─── 拉 ROA (一次批量) ────────────────────────────────────────────────────── #
def _fetch_roa(trade_date: str) -> pd.DataFrame:
    period = _last_quarter(trade_date)
    try:
        return safe_query(
            pro.fina_indicator,
            period=period,                  # 批量按财报期抓
            fields="ts_code,roa"
        )
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})，整列填 0")
        return pd.DataFrame(columns=["ts_code", "roa"])

# ─── 主接口：当天因子池 ──────────────────────────────────────────────────── #
def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()
    logger.info(f"获取交易日 {trade_date} 行情…")

    # A) 日行情
    daily = safe_query(pro.daily, trade_date=trade_date)[
        ["ts_code", "close", "pct_chg", "amount"]
    ]

    # B) daily_basic
    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb,turnover_rate_f"
    )

    # C) ROA
    roa_df = _fetch_roa(trade_date)

    # D) 20 日动量 & 波动率
    start40 = (
        dt.datetime.strptime(trade_date, "%Y%m%d") - dt.timedelta(days=40)
    ).strftime("%Y%m%d")
    hist = safe_query(
        pro.daily,
        start_date=start40,
        end_date=trade_date,
        fields="ts_code,trade_date,pct_chg"
    )
    hist["trade_date"] = pd.to_datetime(hist["trade_date"])

    mom = (hist.set_index("trade_date")
               .groupby("ts_code")["pct_chg"]
               .rolling(20).sum().reset_index())
    mom = mom[mom["trade_date"] == pd.to_datetime(trade_date)][["ts_code","pct_chg"]]\
           .rename(columns={"pct_chg":"pct_chg_20d"})

    vol = (hist.set_index("trade_date")
               .groupby("ts_code")["pct_chg"]
               .rolling(20).std(ddof=0).reset_index())
    vol = vol[vol["trade_date"] == pd.to_datetime(trade_date)][["ts_code","pct_chg"]]\
           .rename(columns={"pct_chg":"vol_20d"})

    df = (daily.merge(basic, on="ts_code")
                .merge(roa_df, on="ts_code", how="left")
                .merge(mom, on="ts_code", how="left")
                .merge(vol, on="ts_code", how="left")
                .fillna(0))
    logger.success(f"行情拉取完成：{len(df)} 条记录")
    return df
