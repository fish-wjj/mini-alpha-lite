#!/usr/bin/env bash
cd /mini-alpha-lite
source venv311/bin/activate
# 仅在最近交易日执行
python - <<'PY'
from src.utils import latest_trade_date
import datetime, os
if datetime.date.today().strftime('%Y%m%d') == latest_trade_date():
    os.system("python -m src.gen_orders")
PY
