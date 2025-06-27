# -*- coding: utf-8 -*-
"""
公共工具：
1) 读取 .env 中的 TuShare TOKEN
2) 获取最近一个交易日
3) 拉取今日所需因子 (PE、PB、20 日动量)
"""
from dotenv import load_dotenv
load_dotenv()                      # 自动把 .env 内容写入环境变量

import os, datetime as dt, pandas as pd, tushare as ts

# —— TuShare 客户端 ——
TS_TOKEN = os.getenv("TUSHARE_TOKEN")
if not TS_TOKEN:
    raise RuntimeError("请先在 .env 中写入 TUSHARE_TOKEN")
pro = ts.pro_api(TS_TOKEN)

# ---------------- 1) 最近交易日 ----------------
def latest_trade_date(back_days: int = 5) -> str:
    """返回最近一个开市日，格式 yyyymmdd"""
    today = dt.date.today()
    for i in range(back_days):
        d = today - dt.timedelta(days=i)
        date_str = d.strftime("%Y%m%d")
        tc = pro.trade_cal(exchange="SSE", start_date=date_str, end_date=date_str)
        if not tc.empty and tc.iloc[0]["is_open"] == 1:
            return date_str
    raise ValueError("未找到最近交易日")

# ---------------- 2) 获取今日因子 DataFrame ----------------
def get_today_universe() -> pd.DataFrame:
    trade_date = latest_trade_date()

    # a) 股票日行情
    daily = pro.daily(trade_date=trade_date)[
        ["ts_code", "close", "pct_chg", "amount"]
    ]

    # b) daily_basic（PE、PB）
    basic = pro.daily_basic(
        trade_date=trade_date, fields="ts_code,pe_ttm,pb"
    )

    # c) 20 日动量
    start_20d = (
        dt.datetime.strptime(trade_date, "%Y%m%d") - dt.timedelta(days=40)
    ).strftime("%Y%m%d")
    mom = pro.daily(
        ts_code="",
        start_date=start_20d,
        end_date=trade_date,
        fields="ts_code,trade_date,pct_chg",
    )
    mom = (
        mom.groupby("ts_code")["pct_chg"]
        .rolling(20)
        .sum()
        .reset_index()
    )
    mom = mom[
        mom["level_1"]
        == mom.groupby("ts_code")["level_1"].transform("max")
    ][["ts_code", "pct_chg"]]
    mom.rename(columns={"pct_chg": "pct_chg_20d"}, inplace=True)

    df = (
        daily.merge(basic, on="ts_code", how="left")
        .merge(mom, on="ts_code", how="left")
        .fillna(0)
    )
    return df
