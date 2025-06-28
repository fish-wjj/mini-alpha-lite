# -*- coding: utf-8 -*-
from dotenv import load_dotenv; load_dotenv()
import os, datetime as dt, pandas as pd, tushare as ts, talib
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger

TOKEN = os.getenv("TUSHARE_TOKEN")
pro = ts.pro_api(TOKEN)
CACHE = Path(__file__).resolve().parent.parent / "data"
CACHE.mkdir(exist_ok=True)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def q(fn, **kw): return fn(**kw)

# ==== 交易日 ====
def latest_trade_date() -> str:
    today = dt.date.today()
    for i in range(5):
        ds = (today-dt.timedelta(days=i)).strftime("%Y%m%d")
        if q(pro.trade_cal, exchange="SSE", start_date=ds, end_date=ds).iloc[0,2]:
            return ds
    raise RuntimeError("找不到交易日")

# ==== 财报期 ====
def _last_quarter(date:str)->str:
    y, m = int(date[:4]), int(date[4:6])
    q = (m-1)//3 or 4
    if q==4 and m<4: y-=1
    return f"{y}{q*3:02d}31"

def _fetch_roa(period:str)->pd.DataFrame:
    fp = CACHE / f"roa_{period}.pkl"
    if fp.exists(): return pd.read_pickle(fp)
    try:
        df = q(pro.fina_indicator, period=period, fields="ts_code,roa")
        df.to_pickle(fp); return df
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})")
        return pd.DataFrame(columns=["ts_code","roa"])

# ==== MACD & MA200 ====
def _macd(close:pd.Series):
    macd, signal, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    return macd - signal

def risk_off(date:str, ma:int=200)->bool:
    start = (dt.datetime.strptime(date,"%Y%m%d")-dt.timedelta(days=ma+250)).strftime("%Y%m%d")
    idx = q(pro.index_daily, ts_code="000300.SH",
            start_date=start,end_date=date,fields="trade_date,close").sort_values("trade_date")
    if len(idx)<ma+3: return False
    close = idx["close"]
    ma200 = close.rolling(ma).mean()
    macd_hist = _macd(close)
    cond = (ma200.iloc[-1] > close.iloc[-1]) and (macd_hist.iloc[-3:]<0).all()
    return bool(cond)

# ==== get_today_universe ====
def get_today_universe() -> pd.DataFrame:
    td = latest_trade_date(); logger.info(f"获取 {td} 行情…")
    daily = q(pro.daily, trade_date=td)[["ts_code","close","pct_chg","amount"]]
    basic = q(pro.daily_basic, trade_date=td,
              fields="ts_code,pe_ttm,pb,turnover_rate_f,total_mv")
    roa   = _fetch_roa(_last_quarter(td))
    start = (dt.datetime.strptime(td,"%Y%m%d")-dt.timedelta(days=40)).strftime("%Y%m%d")
    hist  = q(pro.daily, start_date=start,end_date=td,
              fields="ts_code,trade_date,pct_chg")
    hist["trade_date"]=pd.to_datetime(hist["trade_date"])
    mom = (hist.set_index("trade_date").groupby("ts_code")["pct_chg"]
              .rolling(20).sum().reset_index())
    mom = mom[mom["trade_date"]==pd.to_datetime(td)][["ts_code","pct_chg"]]\
          .rename(columns={"pct_chg":"pct_chg_20d"})
    vol = (hist.set_index("trade_date").groupby("ts_code")["pct_chg"]
              .rolling(20).std(ddof=0).reset_index())
    vol = vol[vol["trade_date"]==pd.to_datetime(td)][["ts_code","pct_chg"]]\
          .rename(columns={"pct_chg":"vol_20d"})
    df = (daily.merge(basic,on="ts_code")
               .merge(roa,on="ts_code",how="left")
               .merge(mom,on="ts_code",how="left")
               .merge(vol,on="ts_code",how="left")
               .fillna(0))
    logger.success(f"行情拉取完成：{len(df)} 条")
    return df
