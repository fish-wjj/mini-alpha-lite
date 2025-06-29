# -*- coding: utf-8 -*-
"""
通用工具集：
* Tushare 安全调用
* 今日 / 指定日 市场截面数据
"""
from __future__ import annotations
import datetime as dt
import os
import json
from functools import wraps
from pathlib import Path

import pandas as pd
import numpy as np
from loguru import logger
import tushare as ts

# ── 全局缓存 ───────────────────────────────────────────────────────────
CACHE: dict[str, pd.DataFrame] = {}

# 读取 token
TOKEN = os.getenv("TUSHARE_TOKEN") or json.load(open(".env.json"))["TUSHARE_TOKEN"]
pro = ts.pro_api(TOKEN)

LOT_STK = 100      # 股票一手
LOT_ETF = 10       # ETF 一手


# ── 安全包装 ───────────────────────────────────────────────────────────
def safe_query(func, **kwargs):
    """简单的指数退避重试"""
    tries = 3
    for i in range(tries):
        try:
            return func(**kwargs)
        except Exception as e:
            if i == tries - 1:
                raise
            logger.warning(f"{func.__name__} 调用失败({e})，第 {i+1}/{tries} 次重试…")


# ── 交易日工具 ─────────────────────────────────────────────────────────
@wraps(pro.trade_cal)
def trade_cal(**kwargs):
    key = f"trade_cal-{kwargs}"
    if key not in CACHE:
        CACHE[key] = safe_query(pro.trade_cal, **kwargs)
    return CACHE[key].copy()

def latest_trade_date() -> str:
    """返回最新开市日（YYYYMMDD）"""
    today = dt.datetime.today().strftime("%Y%m%d")
    cal = trade_cal(exchange='SSE', start_date='20200101', end_date=today)
    return cal[cal["is_open"] == 1]["cal_date"].max()


# ── ROA 拉取（有问题时兜底为 0） ───────────────────────────────────────
def _fetch_roa(ann_date: str) -> pd.DataFrame:
    try:
        df = safe_query(
            pro.fina_indicator,
            period=ann_date,
            fields="ts_code,roa"
        )
        return df
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})，整列填 0")
        return pd.DataFrame({"ts_code": [], "roa": []})


def _build_universe(trade_date: str) -> pd.DataFrame:
    """按指定交易日构建全市场截面"""
    daily = safe_query(
        pro.daily,
        trade_date=trade_date,
        fields="ts_code,close,pct_chg,amount"
    )
    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb,turnover_rate_f,total_mv,industry"
    )
    roa = _fetch_roa(_last_quarter(trade_date))

    # —— 计算 20d / 126d 动量 & 20d 波动率 ——
    start = (pd.to_datetime(trade_date) - dt.timedelta(days=260)).strftime("%Y%m%d")
    hist = safe_query(
        pro.daily,
        start_date=start,
        end_date=trade_date,
        fields="ts_code,trade_date,pct_chg"
    )
    if hist.empty:
        daily["pct_chg_20d"] = 0
        daily["pct_chg_126d"] = 0
        daily["vol_20d"] = 0
        df = daily
    else:
        hist["trade_date"] = pd.to_datetime(hist["trade_date"])
        g = hist.set_index("trade_date").groupby("ts_code")["pct_chg"]
        mom20 = g.rolling(20).sum().reset_index()
        mom126 = g.rolling(126).sum().reset_index()
        vol20 = g.rolling(20).std(ddof=0).reset_index()

        mom20 = mom20[mom20["trade_date"] == pd.to_datetime(trade_date)].rename(columns={"pct_chg": "pct_chg_20d"})
        mom126 = mom126[mom126["trade_date"] == pd.to_datetime(trade_date)].rename(columns={"pct_chg": "pct_chg_126d"})
        vol20 = vol20[vol20["trade_date"] == pd.to_datetime(trade_date)].rename(columns={"pct_chg": "vol_20d"})

        df = (daily.merge(basic, on="ts_code")
                    .merge(roa, on="ts_code", how="left")
                    .merge(mom20[["ts_code", "pct_chg_20d"]], on="ts_code", how="left")
                    .merge(mom126[["ts_code", "pct_chg_126d"]], on="ts_code", how="left")
                    .merge(vol20[["ts_code", "vol_20d"]], on="ts_code", how="left")
                    .fillna(0))

    logger.success(f"行情截面 {trade_date} → {len(df)} 条")
    return df


# ── 对外接口 ───────────────────────────────────────────────────────────
def get_today_universe() -> pd.DataFrame:
    """最新交易日全市场截面"""
    return _build_universe(latest_trade_date())

def get_universe_on(trade_date: str | pd.Timestamp) -> pd.DataFrame:
    """指定交易日截面"""
    trade_date = pd.Timestamp(trade_date).strftime("%Y%m%d")
    return _build_universe(trade_date)


# ── 私有辅助 ───────────────────────────────────────────────────────────
def _last_quarter(ymd: str) -> str:
    """给定 YYYYMMDD 返回上一季报的 period 字符串"""
    d = dt.datetime.strptime(ymd, "%Y%m%d")
    q = (d.month - 1) // 3  # 0~3
    q_end_month = (q * 3) or 12
    year = d.year if q else d.year - 1
    return f"{year}{q_end_month:02d}31"
