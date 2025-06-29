# -*- coding: utf-8 -*-
"""
vectorbt 回测 – 纯 TuShare 行情，无 yfinance
------------------------------------------------
• 支持 ETF + α 个股 (Top-N 轮动)
• 数据首次下载后存 data/price_xxx.parquet，后续直接读取
• 费用：手续费 0.03 %＋滑点 0.1 %
"""
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import vectorbt as vbt
from tqdm import tqdm

from src.config import load_cfg
from src.factor_model import score
from src.utils import get_today_universe, latest_trade_date, q, pro, CACHE

cfg = load_cfg()
PRICE_CACHE = CACHE / "price.parquet"


# ---------- 1. 拉取或读取 ETF 收盘价 ----------
def fetch_price_tushare(ts_code: str, start: str, end: str) -> pd.Series:
    """自动识别 ETF / 股票接口"""
    if ts_code.startswith("51") or ts_code.startswith("15"):
        df = q(pro.fund_daily, ts_code=ts_code, start_date=start, end_date=end, fields="trade_date,close")
    else:
        df = q(pro.daily, ts_code=ts_code, start_date=start, end_date=end, fields="trade_date,close")
    if df.empty:
        raise ValueError(f"No data for {ts_code}")
    return df.set_index("trade_date")["close"].astype(float).sort_index()

def get_price_df(codes: list[str], start: str, end: str) -> pd.DataFrame:
    if PRICE_CACHE.exists():
        price = pd.read_parquet(PRICE_CACHE)
        if set(codes).issubset(price.columns) and price.index.max() >= pd.to_datetime(end):
            return price[codes].loc[start:end]
    # 否则重新拉取
    data = {}
    for c in tqdm(codes, desc="下载行情"):
        data[c] = fetch_price_tushare(c, start, end)
    price = pd.DataFrame(data)
    price.to_parquet(PRICE_CACHE)
    return price

# ---------- 2. 构建再平衡信号 ----------
start = "2018-01-01"
end_dt = dt.datetime.strptime(latest_trade_date(), "%Y%m%d")
codes = [cfg["core_etf"], cfg["bond_etf"]]

price_etf = get_price_df(codes, start, end_dt.strftime("%Y-%m-%d"))

# α 仓：每周一选 Top-N
uni_dates = price_etf.index[price_etf.index.weekday == 0]  # 周一
alpha_weight = (
    pd.DataFrame(index=price_etf.index, columns=[], dtype=float)
)

print("➜  生成 α 轮动权重 …")
for d in tqdm(uni_dates):
    td = d.strftime("%Y%m%d")
    df = get_today_universe()
    top = score(df).head(cfg["num_alpha"])
    codes_alpha = top["ts_code"].tolist()
    price_alpha = get_price_df(codes_alpha, d.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    # 把 alpha 价格拼进总 price
    price_etf = price_etf.reindex(columns=price_etf.columns.union(price_alpha.columns))
    price_etf.loc[price_alpha.index, price_alpha.columns] = price_alpha
    w = pd.Series(cfg["alpha_ratio"] / len(codes_alpha), index=codes_alpha)
    alpha_weight.loc[d, w.index] = w

alpha_weight = alpha_weight.fillna(0)

# ---------- 3. 组合权重：核心 + 债券 + α ----------
w_core = pd.Series(cfg["core_ratio"], index=price_etf.index, name=cfg["core_etf"])
w_bond = pd.Series(cfg["bond_ratio"], index=price_etf.index, name=cfg["bond_etf"])

weights = pd.concat([w_core, w_bond], axis=1).fillna(0)
weights = weights.reindex(columns=price_etf.columns).fillna(0)
weights = weights.add(alpha_weight, fill_value=0)

# ---------- 4. vectorbt 逐日撮合 ----------
pf = vbt.Portfolio.from_weights(
    price_etf.ffill(),
    weights=weights,
    rebalance=True,
    freq="D",
    fees=0.0003,
    slippage=0.001,
)

stats = pf.stats()
print(stats[["Start", "End", "Total Return [%]", "Annualized Return [%]",
             "Max Drawdown [%]", "Max Drawdown Duration"]])

# 保存报告
rep_dir = Path(__file__).resolve().parent.parent / "reports"
rep_dir.mkdir(exist_ok=True)
stats.to_csv(rep_dir / "vbt_stats.csv")
pf.drawdown_underwater().figure().write_image(rep_dir / "underwater.png")
pf.plot().write_html(str(rep_dir / "equity_curve.html"))

print("✓ 回测完成 → reports/vbt_stats.csv & equity_curve.html")
