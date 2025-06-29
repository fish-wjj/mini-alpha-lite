# -*- coding: utf-8 -*-
from dotenv import load_dotenv; load_dotenv()
import os, datetime as dt, numpy as np, pandas as pd, tushare as ts
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger

pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))
CACHE = Path(__file__).resolve().parent.parent / "data"; CACHE.mkdir(exist_ok=True)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def q(fn, **kw): return fn(**kw)

# ---------- 交易日 ----------
def latest_trade_date() -> str:
    today = dt.date.today()
    for i in range(5):
        ds = (today - dt.timedelta(days=i)).strftime("%Y%m%d")
        if q(pro.trade_cal, exchange="SSE", start_date=ds, end_date=ds).iloc[0, 2]:
            return ds
    raise RuntimeError("找不到最近交易日")

# ---------- ROA 缓存 ----------
def _last_quarter(date: str) -> str:
    y, m = int(date[:4]), int(date[4:6])
    qtr = (m - 1) // 3 or 4
    if qtr == 4 and m < 4: y -= 1
    return f"{y}{qtr*3:02d}31"

def _fetch_roa(period: str) -> pd.DataFrame:
    fp = CACHE / f"roa_{period}.pkl"
    if fp.exists(): return pd.read_pickle(fp)
    try:
        df = q(pro.fina_indicator, period=period, fields="ts_code,roa")
        df.to_pickle(fp); return df
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})，整列填 0")
        return pd.DataFrame(columns=["ts_code", "roa"])

# ---------- NumPy MACD ----------
def macd_hist(close: pd.Series, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    return dif - dea

def risk_off(date: str, ma: int = 200) -> bool:
    start = (dt.datetime.strptime(date,"%Y%m%d") - dt.timedelta(days=ma+250)).strftime("%Y%m%d")
    idx = q(pro.index_daily, ts_code="000300.SH",
            start_date=start, end_date=date,
            fields="trade_date,close").sort_values("trade_date")
    if len(idx) < ma + 3: return False
    close = idx["close"]; ma200 = close.rolling(ma).mean()
    hist = macd_hist(close)
    return bool((close.iloc[-1] < ma200.iloc[-1]) and (hist.iloc[-3:] < 0).all())

# ---------- 通用滚动计算 ----------
def _calculate_rolling(df: pd.DataFrame, window: int, func: str) -> pd.DataFrame:
    grouped = df.set_index("trade_date").groupby("ts_code")["pct_chg"]
    if func == 'sum':
        res = grouped.rolling(window).sum().reset_index()
    elif func == 'std':
        res = grouped.rolling(window).std(ddof=0).reset_index()
    else:
        raise ValueError("Unsupported func")
    latest = pd.to_datetime(df['trade_date'].max())
    return res[res["trade_date"] == latest][["ts_code", "pct_chg"]]

# ---------- 截面数据 ----------
def get_today_universe() -> pd.DataFrame:
    td = latest_trade_date(); logger.info(f"获取 {td} 行情…")
    daily = q(pro.daily, trade_date=td)[["ts_code","close","pct_chg","amount"]]
    basic = q(pro.daily_basic, trade_date=td,
              fields="ts_code,pe_ttm,pb,turnover_rate_f,total_mv")
    roa = _fetch_roa(_last_quarter(td))

    start = (dt.datetime.strptime(td,"%Y%m%d") - dt.timedelta(days=40)).strftime("%Y%m%d")
    hist = q(pro.daily, start_date=start, end_date=td,
             fields="ts_code,trade_date,pct_chg")
    if hist.empty:
        df = daily.merge(basic,on="ts_code").merge(roa,on="ts_code",how="left").fillna(0)
        df["pct_chg_20d"] = 0; df["vol_20d"] = 0
    else:
        hist["trade_date"] = pd.to_datetime(hist["trade_date"])
        mom = _calculate_rolling(hist.copy(), 20, 'sum').rename(columns={"pct_chg":"pct_chg_20d"})
        vol = _calculate_rolling(hist.copy(), 20, 'std').rename(columns={"pct_chg":"vol_20d"})
        df = (daily.merge(basic,on="ts_code")
                   .merge(roa,on="ts_code",how="left")
                   .merge(mom,on="ts_code",how="left")
                   .merge(vol,on="ts_code",how="left")
                   .fillna(0))

    logger.success(f"行情拉取完成：{len(df)} 条")
    return df
