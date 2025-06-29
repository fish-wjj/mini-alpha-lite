# -*- coding: utf-8 -*-
"""
周频再平衡脚本（含持仓状态更新）
------------------------------------------------
• 金叉/死叉动态 core_ratio
• 止损 / 止盈（10%）自动写卖单
• 买入/卖出后实时写回 state_portfolio.json
"""
import json, datetime as dt, pathlib, pandas as pd
from src.config import load_cfg
from src.utils import get_today_universe, risk_off, latest_trade_date
from src.factor_model import score
from src.logger import logger

cfg = load_cfg()
BASE = pathlib.Path(__file__).resolve().parent.parent
STATE_FP = BASE / "state_portfolio.json"
CSV_DIR  = BASE / "orders"; CSV_DIR.mkdir(exist_ok=True)
LOT_STK  = cfg["lot"]          # 100
LOT_BOND = 10                  # 债券 ETF 一手 10 份

# ---------- 今日截面 ----------
td = latest_trade_date()
df = get_today_universe()                   # 拉取因子+行情
is_risk_off = risk_off(td)

# ---------- 生成 α 池 ----------
alpha_codes = []
if not is_risk_off:
    alpha_codes = score(df).head(cfg["num_alpha"])["ts_code"].tolist()

# ---------- 组合买入权重 ----------
core_r  = cfg["core_ratio"]
bond_r  = cfg["bond_ratio"]
alpha_r = cfg["alpha_ratio"]

orders = []

# ----------- ETF 买单 -----------
def _add_etf(code, ratio, lot):
    px = df.loc[df["ts_code"] == code, "close"].iat[0]
    qty = int(cfg["cash"] * ratio // (px * lot)) * lot
    if qty > 0:
        orders.append([code.split(".")[0], "B", 0, qty])
_add_etf(cfg["core_etf"], core_r,  LOT_STK)
_add_etf(cfg["bond_etf"], bond_r,  LOT_BOND)

# ----------- α 买单 -------------
if alpha_codes:
    cash_each = cfg["cash"] * alpha_r / len(alpha_codes)
    for c in alpha_codes:
        px = df.loc[df["ts_code"] == c, "close"].iat[0] * 1.01   # +1% 滑点保护
        qty = int(cash_each // (px * LOT_STK)) * LOT_STK
        if qty >= LOT_STK:
            orders.append([c.split(".")[0], "B", round(px, 2), qty])

# ----------- 读旧持仓 -----------
state = {"equity": cfg["cash"], "max_equity": cfg["cash"], "position": {}}
if STATE_FP.exists():
    state = json.load(STATE_FP.open())

new_pos = state["position"].copy()

# ----------- 止损 / 止盈补卖单 -----------
for c, info in list(new_pos.items()):
    cur_px_s = df.loc[df["ts_code"] == c, "close"]
    if cur_px_s.empty:
        continue
    cur_px = cur_px_s.iat[0]
    ret = (cur_px - info["cost"]) / info["cost"]

    # 已经有卖单就跳过
    if any(o[0] == c.split(".")[0] and o[1] == "S" for o in orders):
        continue

    # 止损
    if ret <= -cfg["stop_loss"]:
        orders.append([c.split(".")[0], "S", 0, info["qty"]])
        del new_pos[c]
        logger.info(f"{c} 跌破止损，清仓")
    # 止盈
    elif ret >= cfg["take_profit"]:
        sell_qty = (info["qty"] // LOT_STK // 2) * LOT_STK
        if sell_qty >= LOT_STK:
            orders.append([c.split(".")[0], "S", 0, sell_qty])
            new_pos[c]["qty"] -= sell_qty
            logger.info(f"{c} 浮盈 ≥{cfg['take_profit']*100:.0f}%，卖出一半 {sell_qty}")

# ----------- 把买单写入持仓 -----------
for sec, direc, price, qty in orders:
    if direc == "B":
        ts_row = df[df["ts_code"].str.startswith(sec)]
        if ts_row.empty:
            continue
        ts_code = ts_row["ts_code"].iat[0]
        cost = price or ts_row["close"].iat[0]   # 市价单用今日收盘代替
        if ts_code in new_pos:
            pos = new_pos[ts_code]
            tot_cost = pos["cost"] * pos["qty"] + cost * qty
            tot_qty  = pos["qty"] + qty
            new_pos[ts_code]["cost"] = tot_cost / tot_qty
            new_pos[ts_code]["qty"]  = tot_qty
        else:
            new_pos[ts_code] = {"cost": cost, "qty": qty}

state["position"] = new_pos
with STATE_FP.open("w", encoding="utf-8") as f:
    json.dump(state, f, indent=2, ensure_ascii=False)

# ----------- 输出 CSV -----------
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn  = CSV_DIR / f"orders_{td}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"≡ 周度再平衡 CSV 已生成 → {fn}")
logger.success(f"≡ 持仓状态已更新 → {STATE_FP}")
