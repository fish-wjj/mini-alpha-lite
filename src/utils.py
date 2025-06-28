# -*- coding: utf-8 -*-
"""
工具函数：
  • latest_trade_date
  • get_today_universe  ← 一次性拼出 6 因子所需全部字段
"""
from dotenv import load_dotenv; load_dotenv()
import os, datetime as dt, pandas as pd, tushare as ts
from tenacity import retry, stop_after_attempt, wait_fixed
from src.logger import logger

pro = ts.pro_api(os.getenv("TUSHARE_TOKEN"))

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def safe_query(fn, **kwargs):
    return fn(**kwargs)

def latest_trade_date() -> str:
    today = dt.date.today()
    for i in range(5):
        d = today - dt.timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        if safe_query(pro.trade_cal, exchange="SSE",
                      start_date=ds, end_date=ds).iloc[0, 2]:
            return ds
    raise RuntimeError("未找到最近交易日")

def _last_quarter(date: str) -> str:
    y, m = int(date[:4]), int(date[4:6])
    q = ((m - 1) // 3) or 4
    if q == 4 and m < 4:
        y -= 1
    return f"{y}0{q}31"        # 例: 20240331 / 20231231

def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()
    logger.info(f"获取交易日 {trade_date} 行情…")

    # —— A) 日行情
    daily = safe_query(pro.daily, trade_date=trade_date)[
        ["ts_code", "close", "pct_chg", "amount"]
    ]

    # —— B) 日 basic 因子
    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb,turnover_rate_f"
    )

    # —— C) 最近财报 ROA
    
    # ...前半保持不变...
def _fetch_roa(trade_date: str) -> pd.DataFrame:
    """优先用 period 批量拉 ROA，失败则返回空表"""
    try:
        return safe_query(
            pro.fina_indicator,
            period=_last_quarter(trade_date),      # 指定财报期
            fields="ts_code,roa"
        )
    except Exception as e:
        logger.warning(f"ROA 拉取失败：{e}，整列填 0")
        return pd.DataFrame(columns=["ts_code","roa"])
# ...后半保持不变...


    # —— D) 20 日动量 & 波动率
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
    mom = mom[mom["trade_date"] == pd.to_datetime(trade_date)][["ts_code", "pct_chg"]]\
           .rename(columns={"pct_chg": "pct_chg_20d"})

    vol = (hist.set_index("trade_date")
               .groupby("ts_code")["pct_chg"]
               .rolling(20).std(ddof=0).reset_index())
    vol = vol[vol["trade_date"] == pd.to_datetime(trade_date)][["ts_code", "pct_chg"]]\
           .rename(columns={"pct_chg": "vol_20d"})

    df = (daily.merge(basic, on="ts_code")
                .merge(roa_df, on="ts_code", how="left")
                .merge(mom, on="ts_code", how="left")
                .merge(vol, on="ts_code", how="left")
                .fillna(0))
    logger.success(f"行情拉取完成：{len(df)} 条记录")
    return df
