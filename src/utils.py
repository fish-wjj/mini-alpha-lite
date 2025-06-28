# -*- coding: utf-8 -*-
from dotenv import load_dotenv; load_dotenv()
import os, datetime as dt, pandas as pd, tushare as ts
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger
from src.config import load_cfg

cfg = load_cfg()
pro  = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def safe_query(fn, **kw): return fn(**kw)

def latest_trade_date() -> str:
    td = dt.date.today()
    for i in range(5):
        d = td - dt.timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        if safe_query(pro.trade_cal, exchange="SSE",
                      start_date=ds, end_date=ds).iloc[0,2]:
            return ds
    raise RuntimeError("找不到最近交易日")

def _last_quarter(date: str) -> str:
    y,m = int(date[:4]), int(date[4:6])
    q = (m-1)//3 or 4
    if q==4 and m<4: y-=1
    return f"{y}{q*3:02d}31"

def _fetch_roa(date:str)->pd.DataFrame:
    try:
        return safe_query(pro.fina_indicator,
                          period=_last_quarter(date),
                          fields="ts_code,roa")
    except Exception as e:
        logger.warning(f"ROA 拉取失败({e})，填 0")
        return pd.DataFrame(columns=["ts_code","roa"])

def is_risk_off(date:str)->bool:
    """沪深300 < MA(200) 则 risk-off"""
    start = (dt.datetime.strptime(date,"%Y%m%d")-dt.timedelta(days=300)).strftime("%Y%m%d")
    idx = safe_query(pro.index_daily, ts_code="000300.SH",
                     start_date=start,end_date=date,
                     fields="trade_date,close").sort_values("trade_date")
    if len(idx)<cfg["trend_ma"]: return False
    ma = idx["close"].tail(cfg["trend_ma"]).mean()
    return idx["close"].iat[-1] < ma

def get_today_universe() -> pd.DataFrame:
    td = latest_trade_date(); logger.info(f"获取 {td} 行情…")
    daily = safe_query(pro.daily, trade_date=td)[["ts_code","close","pct_chg","amount"]]
    basic = safe_query(pro.daily_basic, trade_date=td,
                       fields="ts_code,pe_ttm,pb,turnover_rate_f")
    roa   = _fetch_roa(td)
    start = (dt.datetime.strptime(td,"%Y%m%d")-dt.timedelta(days=40)).strftime("%Y%m%d")
    hist  = safe_query(pro.daily, start_date=start,end_date=td,
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
def ma_cross(trade_date: str, short: int = 50, long: int = 200) -> str:
    """
    判断指数均线状态：
    - 'golden' : 50MA 上穿 200MA
    - 'death'  : 50MA 下穿 200MA
    - 'none'   : 其余
    """
    start = (dt.datetime.strptime(trade_date, "%Y%m%d") - dt.timedelta(days=long+30)).strftime("%Y%m%d")
    idx = safe_query(
        pro.index_daily,
        ts_code="000300.SH",
        start_date=start,
        end_date=trade_date,
        fields="trade_date,close"
    ).sort_values("trade_date")
    if len(idx) < long + 1:
        return "none"
    ma_s = idx["close"].rolling(short).mean()
    ma_l = idx["close"].rolling(long).mean()
    if ma_s.iat[-2] < ma_l.iat[-2] and ma_s.iat[-1] > ma_l.iat[-1]:
        return "golden"
    if ma_s.iat[-2] > ma_l.iat[-2] and ma_s.iat[-1] < ma_l.iat[-1]:
        return "death"
    return "none"
