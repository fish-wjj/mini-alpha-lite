"""
简单把核心表（日行情 + daily_basic）同步进本地 SQLite，
方便你后面做更复杂的因子或回测。不用也行。
"""
import sqlite3, datetime as dt, tushare as ts, os
from tqdm import tqdm
from utils import TS_TOKEN

DB = "db.sqlite"
pro = ts.pro_api(TS_TOKEN)

conn = sqlite3.connect(DB)
c = conn.cursor()
c.execute("""CREATE TABLE IF NOT EXISTS daily (
    ts_code TEXT, trade_date TEXT, close REAL, pct_chg REAL, amount REAL
)""")
c.execute("""CREATE TABLE IF NOT EXISTS basic (
    ts_code TEXT, trade_date TEXT, pe_ttm REAL, pb REAL, roe REAL
)""")
conn.commit()

def daterange(start, end):
    for n in range(int((end-start).days)+1):
        yield start + dt.timedelta(n)

start = dt.date(2024,1,1)        # 可自行拉更早
end   = dt.date.today()

for d in tqdm(list(daterange(start,end))):
    trade_date = d.strftime("%Y%m%d")
    daily = pro.daily(trade_date=trade_date)
    if daily.empty: 
        continue
    daily[["ts_code","trade_date","close","pct_chg","amount"]].to_sql("daily", conn, if_exists="append", index=False)

    basic = pro.daily_basic(trade_date=trade_date, fields="ts_code,trade_date,pe_ttm,pb,roe")
    basic.to_sql("basic", conn, if_exists="append", index=False)

conn.close()
print("历史同步完成")
