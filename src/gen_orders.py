# -*- coding: utf-8 -*-
"""
周频再平衡脚本（含持仓状态更新）
------------------------------------------------
1. 读取最新截面 → 生成买 / 卖 CSV
2. 止损 / 止盈（take_profit, stop_loss）自动写卖单
3. 买卖完成后，把持仓写回 state_portfolio.json
"""
import json, datetime as dt, pathlib, pandas as pd
from src.config import load_cfg
from src.utils import get_today_universe, risk_off, latest_trade_date
from src.factor_model import score
from src.logger import logger

# ---------------- 参数与常量 ----------------
cfg = load_cfg()
BASE = pathlib.Path(__file__).resolve().parent.parent
STATE_FP = BASE / "state_portfolio.json"
CSV_DIR  = BASE / "orders"; CSV_DIR.mkdir(exist_ok=True)

LOT_STK  = cfg["lot"]          # 100 股/手
LOT_BOND = 10                  # 债券 ETF 1 手 10 份

# ---------------- 1) 今日行情 ----------------
td = latest_trade_date()
df = get_today_universe()
is_risk_off = risk_off(td)

# ---------------- 2) α 选股 -----------------
alpha_codes = []
if not is_risk_off:
    alpha_codes = score(df).head(cfg["num_alpha"])["ts_code"].tolist()

# ---------------- 3) 买单列表 ----------------
orders: list[list] = []

def _add_etf(code6: str, ratio: float, lot: int):
    """
    根据 6 位代码前缀匹配 ts_code，支持 510300 / 510300.SH 两种写法
    """
    row = df.loc[df["ts_code"].str.startswith(code6), "close"]
    if row.empty:
        raise RuntimeError(f"找不到 {code6} 当日行情，请检查 ETF 代码或是否停牌")
    px  = row.iat[0]
    qty = int(cfg["cash"] * ratio // (px * lot)) * lot
    if qty >= lot:
        orders.append([code6, "B", 0, qty])

# —— ETF 买单
_add_etf(cfg["core_etf"],  cfg["core_ratio"],  LOT_STK)
_add_etf(cfg["bond_etf"],  cfg["bond_ratio"],  LOT_BOND)

# —— α 买单
if alpha_codes:
    cash_each = cfg["cash"] * cfg["alpha_ratio"] / len(alpha_codes)
    for c in alpha_codes:
        px  = df.loc[df["ts_code"] == c, "close"].iat[0] * 1.01   # +1% 保护滑点
        qty = int(cash_each // (px * LOT_STK)) * LOT_STK
        if qty >= LOT_STK:
            orders.append([c.split(".")[0], "B", round(px, 2), qty])

# ---------------- 4) 止损 / 止盈卖单 ----------------
state = {"equity": cfg["cash"], "max_equity": cfg["cash"], "position": {}}
if STATE_FP.exists():
    state = json.load(STATE_FP.open())

new_pos = state["position"].copy()

for code, info in list(new_pos.items()):
    cur_px_s = df.loc[df["ts_code"] == code, "close"]
    if cur_px_s.empty:
        continue
    cur_px = cur_px_s.iat[0]
    ret = (cur_px - info["cost"]) / info["cost"]
    code6 = code.split(".")[0]

    if any(o[0] == code6 and o[1] == "S" for o in orders):
        continue  # 已有卖单

    # 止损
    if ret <= -cfg["stop_loss"]:
        orders.append([code6, "S", 0, info["qty"]])
        del new_pos[code]
        logger.info(f"{code} 跌破止损，清仓")

    # 止盈（卖出一半）
    elif ret >= cfg["take_profit"]:
        sell_qty = (info["qty"] // LOT_STK // 2) * LOT_STK
        if sell_qty >= LOT_STK:
            orders.append([code6, "S", 0, sell_qty])
            new_pos[code]["qty"] -= sell_qty
            logger.info(f"{code} 浮盈 ≥{cfg['take_profit']*100:.0f}%，卖出一半 {sell_qty}")

# ---------------- 5) 把买单加入持仓 ----------------
for sec, direc, price, qty in orders:
    if direc == "B":
        ts_row = df[df["ts_code"].str.startswith(sec)]
        if ts_row.empty:
            continue
        ts_code = ts_row["ts_code"].iat[0]
        cost = price or ts_row["close"].iat[0]
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

# ---------------- 6) 输出 CSV ----------------
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn = CSV_DIR / f"orders_{td}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"≡ 周度再平衡 CSV 已生成 → {fn}")
logger.success(f"≡ 持仓状态已更新 → {STATE_FP}")
