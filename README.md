## Quick start

```bash
git clone https://github.com/fish-wjj/mini-alpha-lite.git
cd mini-alpha-lite
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # 填入你的 TUSHARE_TOKEN
python src/gen_orders.py   # → orders_YYYYMMDD.csv
