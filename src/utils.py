# -*- coding: utf-8 -*-
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
    if q == 4 and m < 4:  # 1月2月属上一年Q4
        y -= 1
    return f"{y}0{q}31"

def _fetch_roa(trade_date: str) -> pd.DataFrame:
    quarter = _last_quarter(trade_date)
    try:
        # 尝试批量拉全市场
        return safe_query(
            pro.fina_indicator,
            ts_code='',
            end_date=quarter,
            fields="ts_code,roa"
        )
    except Exception as e:
        logger.warning(f"ROA 批量拉取失败：{e}，降级用 ann_date")
        try:
            return safe_query(
                pro.fina_indicator,
                ann_date=trade_date,
                fields="ts_code,roa"
            )
        except Exception as e2:
            logger.error(f"ROA ann_date 拉取也失败：{e2}，将填充 0")
            return pd.DataFrame(columns=["ts_code", "roa"])

def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()
    logger.info(f"获取交易日 {trade_date} 行情…")

    daily = safe_query(pro.daily, trade_date=trade_date)[
        ["ts_code", "close", "pct_chg", "amount"]
    ]

    basic = safe_query(
        pro.daily_basic,
        trade_date=trade_date,
        fields="ts_code,pe_ttm,pb,turnover_rate_f"
    )

    # —— ROA —— #
    roa_df = _fetch_roa(trade_date)

    # —— 20 日动量 & 波动率 —— #
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
