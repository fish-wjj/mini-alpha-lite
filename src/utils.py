# -*- coding: utf-8 -*-
"""
工具函数
1. 读取 TuShare TOKEN
2. 取最近交易日
3. 拉取当日因子：PE、PB、20 日动量
   - 带自动重试：API 报错或限流时，最多重试 3 次
"""
from dotenv import load_dotenv

load_dotenv()  # 把 .env 写进 os.environ

import os
import datetime as dt
import pandas as pd
import tushare as ts
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger

# ─── TuShare 客户端 ─────────────────────────────────────────────────────────── #
TS_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TS_TOKEN:
    raise RuntimeError("缺少 TUSHARE_TOKEN，请在 .env 中配置")
pro = ts.pro_api(TS_TOKEN)


# ─── 自动重试封装 ──────────────────────────────────────────────────────────── #
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def safe_query(func, **kwargs):
    """对 TuShare 查询做多次重试"""
    return func(**kwargs)


# ─── 最近交易日 ───────────────────────────────────────────────────────────── #
def latest_trade_date(back_days: int = 5) -> str:
    today = dt.date.today()
    for i in range(back_days):
        d = today - dt.timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        tc = safe_query(
            pro.trade_cal, exchange="SSE", start_date=ds, end_date=ds
        )
        if not tc.empty and tc.iloc[0]["is_open"] == 1:
            return ds
    raise RuntimeError("未找到最近交易日")


# ─── 当日因子池 ───────────────────────────────────────────────────────────── #
def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()
    logger.info(f"获取交易日 {trade_date} 行情…")

    daily = safe_query(
        pro.daily, trade_date=trade_date
    )[
        ["ts_code", "close", "pct_chg", "amount"]
    ]

    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb",
    )

    # 20 日动量
    start = (
        dt.datetime.strptime(trade_date, "%Y%m%d") - dt.timedelta(days=40)
    ).strftime("%Y%m%d")
    mom_raw = safe_query(
        pro.daily,
        start_date=start,
        end_date=trade_date,
        fields="ts_code,trade_date,pct_chg",
    )
    mom = (
        mom_raw.groupby("ts_code")["pct_chg"]
        .rolling(20)
        .sum()
        .reset_index()
    )
    mom = mom[
        mom["level_1"]
        == mom.groupby("ts_code")["level_1"].transform("max")
    ][["ts_code", "pct_chg"]].rename(columns={"pct_chg": "pct_chg_20d"})

    df = (
        daily.merge(basic, on="ts_code", how="left")
        .merge(mom, on="ts_code", how="left")
        .fillna(0)
    )
    logger.success(f"行情拉取完成：{len(df)} 条记录")
    return df
