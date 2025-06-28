# -*- coding: utf-8 -*-
import os, datetime as dt, pandas as pd, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
from src.utils import get_today_universe, latest_trade_date, safe_query, pro, is_risk_off
from src.factor_model import score
from src.config import load_cfg; cfg=load_cfg()

START = "20180101"
REPORT = Path(__file__).resolve().parent.parent/"reports"
REPORT.mkdir(exist_ok=True)

def trade_days():
    cal=safe_query(pro.trade_cal,exchange="SSE",
                   start_date=START,end_date=latest_trade_date())
    return pd.to_datetime(cal[cal["is_open"]==1]["cal_date"]).sort_values()

def px(code,date,is_fund=False):
    api=pro.fund_daily if is_fund else pro.daily
    df=safe_query(api,ts_code=code,end_date=date,limit=1,fields="close")
    return None if df.empty else float(df["close"].iat[0])

days=trade_days(); month_start=days[days.dt.day==1]
print(f"回测区间：{month_start.iloc[0].date()} → {month_start.iloc[-1].date()}")

equity=[1.0]; dates=[]
for d in tqdm(month_start):
    trd=d.strftime("%Y%m%d"); risk=is_risk_off(trd)
    uni=get_today_universe()
    ranked = (
        score(uni).head(cfg["num_alpha"])
        if not risk
        else pd.DataFrame(columns=["ts_code"])
    )
    alpha=ranked["ts_code"].tolist()
    end=(d+pd.offsets.MonthEnd()).strftime("%Y%m%d")
    if end>latest_trade_date(): break
    r_etf=(px(cfg["core_etf"],end,True)-px(cfg["core_etf"],trd,True))/px(cfg["core_etf"],trd,True)
    r_bond=(px(cfg["bond_etf"],end,True)-px(cfg["bond_etf"],trd,True))/px(cfg["bond_etf"],trd,True)
    r_stock=np.mean([(px(c,end)-px(c,trd))/px(c,trd) for c in alpha]) if alpha else 0
    ret=(cfg["core_ratio"]*r_etf + cfg["bond_ratio"]*r_bond + cfg["alpha_ratio"]*r_stock)
    equity.append(equity[-1]*(1+ret)); dates.append(pd.to_datetime(end))

rep=pd.DataFrame({"date":dates,"equity":equity[1:]})
rep["ret"]=rep["equity"].pct_change().fillna(0)
rep["cummax"]=rep["equity"].cummax(); rep["drawdown"]=rep["equity"]/rep["cummax"]-1
rep.to_csv(REPORT/"backtest_report.csv",index=False)
plt.figure(figsize=(10,4)); plt.plot(rep["date"],rep["equity"]); plt.tight_layout()
plt.savefig(REPORT/"equity_curve.png")
print("回测完成 → reports/backtest_report.csv & equity_curve.png")
