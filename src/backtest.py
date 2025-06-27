# -*- coding: utf-8 -*-
"""
滚动回测：2018-01-01 → today
输出 equity 曲线、收益、最大回撤
"""
import datetime as dt, backtrader as bt, pandas as pd
from pathlib import Path
from src.utils import safe_query, latest_trade_date
from src.factor_model import score
from src.logger import logger

START = "20180101"

class AlphaStrategy(bt.Strategy):
    params = dict(core_ratio=0.6, alpha_ratio=0.3, num_alpha=5)

    def __init__(self):
        self.order = None

    def next(self):
        # 每月第一个交易日调仓
        if self.data.datetime.date(0).day != 1: return
        df = self.datas[0].lines.df[0]    # 日线 DataFrame
        ranked = score(df).head(self.p.num_alpha)
        self.rebalance(ranked)

    def rebalance(self, ranked):
        tgt = {}
        cash = self.broker.getvalue()
        core_cash = cash * self.p.core_ratio
        alpha_cash_each = cash * self.p.alpha_ratio / self.p.num_alpha
        for row in ranked.itertuples():
            tgt[row.ts_code] = alpha_cash_each / row.close
        # 生成买卖单…
        # 省略 ⇢ 完整脚本我可后续补

if __name__ == "__main__":
    cerebro = bt.Cerebro()
    # 省略：加载历史行情 → pandas → feed
    result = cerebro.run()
    cerebro.plot()
