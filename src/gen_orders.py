# -*- coding: utf-8 -*-
import json, datetime as dt, pathlib, pandas as pd
from src.config import load_cfg; cfg=load_cfg()
from src.utils import get_today_universe, pro, is_risk_off
from src.factor_model import score
from src.logger import logger

BASE=pathlib.Path(__file__).resolve().parent.parent
CSV_DIR=BASE/"orders"; CSV_DIR.mkdir(exist_ok=True)
STATE_FP=BASE/"state_portfolio.json"

td=dt.date.today().strftime("%Y%m%d")
df=get_today_universe(); risk=is_risk_off(td)

# 股票池过滤
stock=df[~df["ts_code"].str.startswith(("15","51","56")) & (df["amount"]>=cfg["min_amount"])]
if stock.empty: stock=df[~df["ts_code"].str.startswith(("15","51","56"))]
etfs=df[df["ts_code"].str.startswith(("15","51","56"))]
df_filt=pd.concat([stock,etfs])

alpha=[]
if not risk:
    alpha=score(df_filt).head(cfg["num_alpha"])["ts_code"].tolist()
else:
    logger.warning("大盘跌破 200MA，仅持 ETF")

orders=[]
LOT=cfg["lot"]

# core ETF
core_px=pro.fund_daily(ts_code=cfg["core_etf"],limit=1)["close"].iat[0]
core_qty=int(cfg["cash"]*cfg["core_ratio"]//(core_px*LOT))*LOT
orders.append([cfg["core_etf"].split(".")[0],"B",0,core_qty])

# bond ETF
bond_px=pro.fund_daily(ts_code=cfg["bond_etf"],limit=1)["close"].iat[0]
bond_qty=int(cfg["cash"]*cfg["bond_ratio"]//(bond_px*LOT))*LOT
orders.append([cfg["bond_etf"].split(".")[0],"B",0,bond_qty])

# α buy
alpha_cash=cfg["cash"]*cfg["alpha_ratio"]/max(len(alpha),1)
for code in alpha:
    price=df.loc[df["ts_code"]==code,"close"].iat[0]*1.01
    qty=int(alpha_cash//(price*LOT))*LOT
    if qty>=LOT: orders.append([code.split(".")[0],"B",round(price,2),qty])

# 输出
csv_fp=CSV_DIR/f"orders_{td}.csv"
pd.DataFrame(orders,columns=["证券代码","买卖标志","委托价格","委托数量"]).to_csv(csv_fp,index=False,encoding="utf-8-sig")
logger.success(f"CSV 已生成 → {csv_fp}")
