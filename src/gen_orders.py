# -*- coding: utf-8 -*-
"""
生成批量下单 CSV（含风控逻辑）
• 参数全部来自 config.yaml
• 若当日无满足成交额的股票 → 自动放宽 amount 门槛
• 单票止损、组合回撤保护
"""
import datetime as dt, json, pathlib, pandas as pd
from src.logger import logger
from src.utils import pro, get_today_universe
from src.factor_model import score
from src.config import load_cfg

# ─── 加载配置 ───────────────────────────────────────────────────────────── #
cfg = load_cfg()
LOT = int(cfg["lot"])

BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
STATE_FP = BASE_DIR / "state_portfolio.json"
CSV_DIR  = BASE_DIR / "orders"
CSV_DIR.mkdir(exist_ok=True)

# ─── 今日行情 ──────────────────────────────────────────────────────────── #
today = dt.date.today().strftime("%Y%m%d")
df = get_today_universe()

# 1) 股票成交额过滤；若结果为空则放宽门槛
stock_mask = ~df["ts_code"].str.startswith(("15", "51", "56"))
stock_df   = df[stock_mask & (df["amount"] >= cfg["min_amount"])]
if stock_df.empty:
    logger.warning("成交额过滤后无股票可选，已放宽 amount 门槛")
    stock_df = df[stock_mask]

df_filt = pd.concat(
    [stock_df, df[~stock_mask]]  # 股票 + ETF
)

# 2) 评分选股
alpha_df = score(df_filt).head(cfg["num_alpha"])
alpha_list = alpha_df["ts_code"].tolist()

# 3) 读取昨日持仓
if STATE_FP.exists():
    state = json.load(STATE_FP.open())
else:
    state = {"equity": 1.0, "max_equity": 1.0, "position": {}}

# 4) 组合回撤风控
drawdown = 1 - state["equity"] / max(state["max_equity"], state["equity"])
if drawdown > cfg["max_drawdown"]:
    logger.warning(f"组合回撤 {drawdown:.2%} 超阈值，暂停 α 仓买入")
    alpha_list = []

# ─── 生成订单 ───────────────────────────────────────────────────────────── #
orders = []

# 核心 ETF
etf_code = cfg["core_etf"]
etf_px   = pro.fund_daily(ts_code=etf_code, limit=1)["close"].iat[0]
etf_qty  = int(cfg["cash"]*cfg["core_ratio"] // (etf_px*LOT)) * LOT
orders.append([etf_code.split(".")[0], "B", 0, etf_qty])   # 市价

# α 仓买入
alpha_cash_each = cfg["cash"] * cfg["alpha_ratio"] / max(len(alpha_list), 1)
for _, row in alpha_df.iterrows():
    if row["ts_code"] not in alpha_list:
        continue
    px  = round(row["close"] * 1.01, 2)
    qty = int(alpha_cash_each // (px*LOT)) * LOT
    if qty >= LOT:
        orders.append([row["ts_code"].split(".")[0], "B", px, qty])

# 止损卖出
for code, info in state["position"].items():
    if code.startswith(("15","51","56")) or code == etf_code:      # ETF 不止损
        continue
    cur_px = df.loc[df["ts_code"]==code, "close"]
    if not cur_px.empty and (info["cost"]-cur_px.iat[0]) / info["cost"] >= cfg["stop_loss"]:
        orders.append([code.split(".")[0], "S", 0, info["qty"]])
        logger.info(f"{code} 触发止损，市价卖出 {info['qty']}")

# ─── 保存新仓位快照（仅示范 α 仓成本价） ───────────────────────────────── #
new_state = {
    "equity": state["equity"], "max_equity": max(state["max_equity"], state["equity"]),
    "position": {}
}
for o in orders:
    if o[1]=="B" and o[0] != etf_code.split(".")[0]:
        new_state["position"][o[0]+".SZ"] = {"cost": o[2] if o[2]>0 else
                                             df.loc[df["ts_code"].str.startswith(o[0]),"close"].iat[0],
                                             "qty": o[3]}
json.dump(new_state, STATE_FP.open("w"), indent=2)

# ─── 输出 CSV ──────────────────────────────────────────────────────────── #
csv_fp = CSV_DIR / f"orders_{today}.csv"
pd.DataFrame(orders, columns=["证券代码","买卖标志","委托价格","委托数量"])\
  .to_csv(csv_fp, index=False, encoding="utf-8-sig")
logger.success(f"下单文件已生成 → {csv_fp}")
