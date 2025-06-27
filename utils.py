# -*- coding: utf-8 -*-
"""
工具函数
1. 读取 TuShare TOKEN（.env）
2. 取最近交易日 (SSE)
3. 拉当日所需因子：PE、PB、20d 动量
"""
import os
import datetime as dt
import pandas as pd
import tushare as ts

from dotenv import load_dotenv

load_dotenv()  # 把 .env 写进 os.environ

TS_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TS_TOKEN:
    raise RuntimeError("缺少 TUSHARE_TOKEN，请在 .env 中配置")

pro = ts.pro_api(TS_TOKEN)


# ===== 最近交易日 =========================================================== #
def latest_trade_date(back_days: int = 5) -> str:
    """返回最近开市日 yyyyMMdd"""
    today = dt.date.today()
    for i in range(back_days):
        d = today - dt.timedelta(days=i)
        ds = d.strftime("%Y%m%d")
        if pro.trade_cal(exchange="SSE", start_date=ds, end_date=ds).iloc[0, 2]:
            return ds
    raise RuntimeError("5 天内未找到交易日")


# ===== 当日因子池 =========================================================== #
def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()

    daily = pro.daily(trade_date=trade_date)[["ts_code", "close", "pct_chg", "amount"]]

    basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,pe_ttm,pb")

    # 20 日动量
    start = (
        dt.datetime.strptime(trade_date, "%Y%m%d") - dt.timedelta(days=40)
    ).strftime("%Y%m%d")
    mom = (
        pro.daily(
            start_date=start,
            end_date=trade_date,
            fields="ts_code,trade_date,pct_chg",
        )
        .groupby("ts_code")["pct_chg"]
        .rolling(20)
        .sum()
        .reset_index()
    )
    mom = mom[mom["level_1"] == mom.groupby("ts_code")["level_1"].transform("max")][
        ["ts_code", "pct_chg"]
    ].rename(columns={"pct_chg": "pct_chg_20d"})

    return (
        daily.merge(basic, on="ts_code", how="left")
        .merge(mom, on="ts_code", how="left")
        .fillna(0)
    )
