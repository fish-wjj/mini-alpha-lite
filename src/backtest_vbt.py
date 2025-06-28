# -*- coding: utf-8 -*-
import vectorbt as vbt, pandas as pd, datetime as dt
from src.utils import get_today_universe, latest_trade_date, CACHE
from src.factor_model import score
from src.config import load_cfg; cfg=load_cfg()

start="2018-01-01"
end = dt.datetime.strptime(latest_trade_date(), "%Y%m%d")

# 1) 下载指数 & 债券 ETF 日线
codes = [cfg["core_etf"], cfg["bond_etf"]]
price = vbt.YFData.download([c.replace(".SH",".SS") for c in codes],
                            start=start,end=end).get("Close")
price.columns = codes

# 2) α 仓：每周一根据 score 选 top N
close_all = vbt.YFData.download("000001.SS",start=start,end=end).get("Close") # placeholder
# 简化示例：不加载全市场，直接用指数做 α
alpha_weight = pd.Series(0,index=price.index)
alpha_weight.iloc[::5] = cfg["alpha_ratio"]   # 每周一调仓

core_weight  = cfg["core_ratio"]
bond_weight  = cfg["bond_ratio"]

pf = vbt.Portfolio.from_weights(
    price,
    weights = pd.DataFrame({
        cfg["core_etf"]: core_weight,
        cfg["bond_etf"]: bond_weight
    }, index=price.index),
    freq="D",
    fees=0.0003,
    slippage=0.001
)
print(pf.stats())
pf.plot().show()
