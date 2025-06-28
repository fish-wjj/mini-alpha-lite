# -*- coding: utf-8 -*-
"""
根据今日数据生成批量下单 CSV + 风控
• 从 config.yaml 读取参数
• 若持仓股票跌破止损 —— 卖出
• 若组合净值回撤 > max_drawdown —— 清仓 α 仓，仅留 ETF
"""
import datetime as dt, pandas as pd, json, pathlib
from src.logger import logger
from src.utils import pro, get_today_universe
from src.factor_model import score
from src.config import load_cfg

cfg = load_cfg()
LOT = int(cfg["lot"])

# —— 路径
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
STATE_FP = BASE_DIR / "state_portfolio.json"   # 保存上期持仓成本价
CSV_DIR  = BASE_DIR / "orders"
CSV_DIR.mkdir(exist_ok=True)

today = dt.date.today().strftime("%Y%m%d")
df = get_today_universe()

# 1) 过滤金额
df = df[(df["amount"] >= cfg["min_amount"]) | df["ts_code"].str.startswith(("15","51","56"))]

# 2) 评分 & 选股
alpha_df  = score(df).head(cfg["num_alpha"])
alpha_l   = alpha_df["ts_code"].tolist()

# 3) 读取上期持仓
if STATE_FP.exists():
    state = json.load(STATE_FP.open())
else:
    state = {"equity": 1.0, "max_equity": 1.0, "position": {}}

orders = []

# 4) ===== 风控：组合回撤 =====
dd = 1 - state["equity"] / max(state["max_equity"], state["equity"])
if dd > cfg["max_drawdown"]:
    logger.warning(f"组合回撤 {dd:.2%} 超过阈值，清空 α 仓")
    alpha_l = []   # 只留 ETF

# 5) ===== 核心 ETF =====
etf_code = cfg["core_etf"]
etf_px   = pro.fund_daily(ts_code=etf_code, limit=1)["close"].iat[0]
etf_qty  = int(cfg["cash"] * cfg["core_ratio"] // (etf_px * LOT)) * LOT
orders.append([etf_code.split(".")[0], "B", 0, etf_qty])

# 6) ===== α 仓买入 =====
alpha_cash_each = cfg["cash"] * cfg["alpha_ratio"] / max(len(alpha_l),1)
for _, row in alpha_df.iterrows():
    if row["ts_code"] not in alpha_l: continue
    px  = round(row["close"] * 1.01, 2)
    qty = int(alpha_cash_each // (px * LOT)) * LOT
    if qty >= LOT:
        orders.append([row["ts_code"].split(".")[0], "B", px, qty])

# 7) ===== 止损卖出 =====
for code, info in state["position"].items():
    if code.startswith("15") or code==etf_code:  # ETF 不止损
        continue
    cur_px = df.loc[df["ts_code"]==code, "close"]
    if cur_px.empty: continue
    if (info["cost"] - cur_px.iat[0]) / info["cost"] >= cfg["stop_loss"]:
        orders.append([code.split(".")[0], "S", 0, info["qty"]])
        logger.info(f"{code} 触发止损，市价卖出 {info['qty']}")

# 8) 保存新持仓快照（这里只示范记录 α 仓成本）
new_state = {"equity": state["equity"], "max_equity": state["max_equity"], "position": {}}
for o in orders:
    if o[1] == "B" and o[0] != etf_code.split(".")[0]:
        new_state["position"][o[0]+".SZ"] = {"cost": o[2] if o[2]>0 else df.loc[df["ts_code"].str.startswith(o[0]),"close"].iat[0],
                                             "qty":  o[3]}
json.dump(new_state, STATE_FP.open("w"), indent=2)

# 9) 输出 CSV
csv = pd.DataFrame(orders, columns=["证券代码","买卖标志","委托价格","委托数量"])
fn  = CSV_DIR / f"orders_{today}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"下单文件已生成 → {fn}")
