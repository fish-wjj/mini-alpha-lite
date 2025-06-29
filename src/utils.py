# ──────────────────────────────────────────────────────────────
#  utils.py  –  市场数据 / 截面构建 / 通用工具
#  * 集中“因子打分”，让任何调用 build_today_universe()
#    都自动带上一列 df['score']
# ──────────────────────────────────────────────────────────────
from __future__ import annotations

import os, functools, datetime as dt
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from loguru import logger
from dotenv import load_dotenv
import tushare as ts

# ========== 环境变量 & Tushare 客户端 ==========
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")                       # 读取 .env
TS_TOKEN = os.getenv("TUSHARE_TOKEN") or os.getenv("TS_TOKEN")
if not TS_TOKEN:
    raise RuntimeError("请在 .env 中设置 TS_TOKEN 或 TUSHARE_TOKEN")

pro = ts.pro_api(TS_TOKEN)                       # Tushare Pro 客户端

# ========== 通用重试包装 ==========
def safe_query(api_fn: Callable, **kwargs) -> pd.DataFrame:
    """对 Tushare 查询加 3 次重试；出错时返回空 df"""
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def _q():
        logger.debug("tushare → {} {}", api_fn.__name__ if hasattr(api_fn, '__name__') else api_fn, kwargs)
        return api_fn(**kwargs)
    try:
        return _q()
    except Exception as e:                       # noqa: BLE001
        logger.error("tushare 查询失败：{}", e)
        return pd.DataFrame()

# ========== 交易日 & 最新交易日 ==========
def _trade_cal(start: str, end: str) -> pd.Series:
    df = safe_query(pro.trade_cal,
                    exchange="SSE", start_date=start, end_date=end)
    return pd.to_datetime(df[df.is_open == 1]["cal_date"])

def latest_trade_date(n: int = 0) -> str:
    """返回距今天 n 个交易日的日期字符串  YYYYMMDD"""
    today = dt.datetime.today().strftime("%Y%m%d")
    cal   = _trade_cal("20160101", today).sort_values()
    return cal.iloc[-(n+1)].strftime("%Y%m%d")

def prev_trade_date(current_date_str: str) -> str:
    """返回给定日期字符串(YYYYMMDD)的上一个交易日"""
    # 获取到给定日期的所有历史交易日
    cal = _trade_cal("20160101", current_date_str)
    if cal.empty:
        return None
    
    # 将输入日期转为 datetime 对象，以便比较
    current_dt = pd.to_datetime(current_date_str)
    
    # 筛选出严格早于给定日期的交易日并取最后一个
    prev_cal = cal[cal < current_dt]
    if not prev_cal.empty:
        return prev_cal.iloc[-1].strftime("%Y%m%d")
    
    return None

# ========== ROA ==========
def _fetch_roa(ann_date: str) -> pd.DataFrame:
    df = safe_query(pro.fina_indicator, ann_date=ann_date,
                    fields="ts_code,roa")
    if df.empty:                                # 尝试区间拉取
        df = safe_query(pro.fina_indicator,
                        start_date=ann_date, end_date=ann_date,
                        fields="ts_code,roa")
    if df.empty:
        logger.warning("ROA 拉取失败，整列填 0")
        return pd.DataFrame(columns=["ts_code", "roa"])
    df["roa"].fillna(0, inplace=True)
    return df

# ========== 辅助：滚动动量 / 波动率 ==========
def _rolling(df: pd.DataFrame, win: int, how: str) -> pd.DataFrame:
    g = (df.set_index("trade_date")
           .groupby("ts_code")["pct_chg"]
           .rolling(win))
    if how == "sum":
        res = g.sum()
    elif how == "std":
        res = g.std(ddof=0)
    else:
        raise ValueError("how 必须是 'sum' 或 'std'")
    res = res.reset_index()
    last_day = df.trade_date.max()
    res = res[res.trade_date == last_day][["ts_code", "pct_chg"]]
    return res.rename(columns={"pct_chg": f"pct_chg_{win}d" if how=="sum" else f"vol_{win}d"})

# ========== 今天的市场截面 ==========
def build_today_universe(td: str | None = None) -> pd.DataFrame:
    """
    组装单日截面并自动打分：
    返回字段 >>>  原始行情字段 + 各类因子列 + [score]
    """
    td = td or latest_trade_date()
    # ---- 1. 基础行情 ----
    daily   = safe_query(pro.daily,        trade_date=td,
                         fields="ts_code,close,pct_chg,amount")
    basic   = safe_query(pro.daily_basic, trade_date=td,
                         fields="ts_code,pe_ttm,pb,turnover_rate_f,total_mv")

    # ========= ★ 修改点：新增检查逻辑 ★ =========
    if daily.empty or 'ts_code' not in daily.columns:
        logger.warning(f"无法获取 {td} 的日线行情数据，跳过当期截面构建")
        return pd.DataFrame()
    if basic.empty or 'ts_code' not in basic.columns:
        logger.warning(f"无法获取 {td} 的日线基本指标数据，跳过当期截面构建")
        return pd.DataFrame()

    # ---- 2. ROA ----
    quarter = td[:4] + f"{(int(td[4:6])-1)//3*3+1:02}01"     # 取上季度公告日
    roa = _fetch_roa(quarter)

    # ---- 3. 动量 & 波动率（20 日）----
    start = (dt.datetime.strptime(td, "%Y%m%d") - dt.timedelta(days=40)).strftime("%Y%m%d")
    hist  = safe_query(pro.daily, start_date=start, end_date=td,
                       fields="ts_code,trade_date,pct_chg")
    mom = _rolling(hist, 20, "sum") if not hist.empty else pd.DataFrame()
    vol = _rolling(hist, 20, "std") if not hist.empty else pd.DataFrame()

    # ---- 4. 合并 ----
    df = (daily.merge(basic, on="ts_code")
               .merge(roa,  on="ts_code", how="left")
               .merge(mom,  on="ts_code", how="left")
               .merge(vol,  on="ts_code", how="left")
               .fillna(0))

    # ---- 5. 因子打分（关键新增）----
    from src.factor_model import score as factor_score    # 延迟导入避免循环引用
    df = factor_score(df)                                # ← 生成 df['score']

    logger.success(f"行情截面 {td} → {len(df):,} 条")
    return df

# -----------------------------------------------------------------------------
# ★ 修改点：导出 prev_trade_date
__all__ = ["build_today_universe", "latest_trade_date", "prev_trade_date", "safe_query", "pro"]