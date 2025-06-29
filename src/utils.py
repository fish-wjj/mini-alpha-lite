#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具函数集合：
* build_today_universe()                 —— 生成当日（或指定日）截面 dataframe
* latest_trade_date() / prev_trade_date —— 交易日查询
* safe_query()                           —— tushare 带重试的查询
* _fetch_roa()                           —— ROA 因子专用拉取（已做降级和兜底）
"""
from __future__ import annotations

import datetime as dt
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
import tushare as ts
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_random

# ─────────────────── tushare 初始化 ────────────────────
TS_TOKEN = os.getenv("TS_TOKEN", "").strip()
if not TS_TOKEN:
    raise RuntimeError("请在 .env 中设置 TS_TOKEN")
pro = ts.pro_api(TS_TOKEN)

# 缓存目录
CACHE = Path(__file__).resolve().parent.parent / ".cache"
CACHE.mkdir(exist_ok=True)

# ──────────────────── 基础封装 ─────────────────────────
def _today(fmt: str = "%Y%m%d") -> str:
    return dt.datetime.now().strftime(fmt)


@retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=2))
def safe_query(fn, **kwargs) -> pd.DataFrame:
    """给 tushare 接口加上重试 & 日志"""
    name = getattr(fn, "__name__", str(fn))
    logger.debug(f"tushare → {name} {kwargs}")
    return fn(**kwargs)


def latest_trade_date() -> str:
    """返回最近一个交易日（含今天）"""
    today = _today()
    cal = safe_query(
        pro.trade_cal,
        exchange="SSE",
        start_date=(dt.datetime.today() - dt.timedelta(days=7)).strftime("%Y%m%d"),
        end_date=today,
    )
    trade_days = cal[cal.is_open == 1]["cal_date"].tolist()
    return trade_days[-1]


def prev_trade_date(date: str | None = None) -> str:
    """返回 date 的前一个交易日；date 为空 => 取今天"""
    if date is None:
        date = latest_trade_date()
    cal = safe_query(
        pro.trade_cal,
        exchange="SSE",
        start_date=(dt.datetime.strptime(date, "%Y%m%d") - dt.timedelta(days=10)).strftime("%Y%m%d"),
        end_date=date,
    )
    trade_days = cal[cal.is_open == 1]["cal_date"].tolist()
    return trade_days[-2]


# ──────────────────── ROA 单独处理 ─────────────────────
@lru_cache(maxsize=8)
def _fetch_roa(ann_date: str) -> pd.DataFrame:
    """
    取最近一次季报 ROA。如全部失败则返回空 df，外层会填 0.
    """
    try:
        roa_df = safe_query(
            pro.fina_indicator,
            ann_date=ann_date,
            fields="ts_code,roa"
        )
        if not roa_df.empty:
            return roa_df
    except Exception as e:
        logger.warning(f"ROA 批量拉取失败({e})，尝试按季度…")

    # 降级：以报表期查
    try:
        roa_df = safe_query(
            pro.fina_indicator,
            start_date=ann_date, end_date=ann_date,
            fields="ts_code,roa"
        )
        return roa_df
    except Exception as e:
        logger.error(f"ROA 降级拉取仍失败({e})，返回空 df")
        return pd.DataFrame(columns=["ts_code", "roa"])


# ─────────────────── 今日截面 ──────────────────────────
def _calc_roll(df: pd.DataFrame, win: int, func: str) -> pd.DataFrame:
    """
    df: ts_code, trade_date, pct_chg
    返回：ts_code, value
    """
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    latest = df.trade_date.max()
    g = df.set_index("trade_date").groupby("ts_code")["pct_chg"]
    if func == "sum":
        out = g.rolling(win).sum().reset_index()
    elif func == "std":
        out = g.rolling(win).std(ddof=0).reset_index()
    else:
        raise ValueError("func only support sum/std")
    return out[out.trade_date == latest][["ts_code", "pct_chg"]]


def build_today_universe(trade_date: str | None = None) -> pd.DataFrame:
    """返回今日(或指定日)截面 dataframe，含基本面与技术面因子（股票池为A股全市场）"""
    td = trade_date or latest_trade_date()
    prev_td = prev_trade_date(td)

    daily = safe_query(
        pro.daily, trade_date=td, fields="ts_code,close,pct_chg,amount"
    )
    basic = safe_query(
        pro.daily_basic,
        trade_date=td,
        fields="ts_code,pe_ttm,pb,turnover_rate_f,total_mv",
    )
    roa = _fetch_roa(_today("%Y%m") + "01")  # 退一步：本月1号近似为最新公告

    # 20日动量 & 波动率
    hist = safe_query(
        pro.daily,
        start_date=(dt.datetime.strptime(prev_td, "%Y%m%d") - dt.timedelta(days=40)).strftime("%Y%m%d"),
        end_date=prev_td,
        fields="ts_code,trade_date,pct_chg",
    )
    if hist.empty:
        mom = pd.DataFrame(columns=["ts_code", "pct_chg_20d"])
        vol = pd.DataFrame(columns=["ts_code", "vol_20d"])
    else:
        mom = _calc_roll(hist.copy(), 20, "sum").rename(columns={"pct_chg": "pct_chg_20d"})
        vol = _calc_roll(hist, 20, "std").rename(columns={"pct_chg": "vol_20d"})

    df = (
        daily.merge(basic, on="ts_code")
        .merge(roa, on="ts_code", how="left")
        .merge(mom, on="ts_code", how="left")
        .merge(vol, on="ts_code", how="left")
    )
    df["roa"].fillna(0, inplace=True)
    df[["pct_chg_20d", "vol_20d"]] = df[["pct_chg_20d", "vol_20d"]].fillna(0)
    logger.success(f"行情截面 {td} → {len(df):,} 条")
    return df


# 兼容旧名字
_build_universe = build_today_universe

# 例如
from some_orm import q
