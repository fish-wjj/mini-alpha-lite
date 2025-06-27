# mini-alpha-lite 🐟

> **一条命令** 输出 A 股批量下单 CSV（核心红利 ETF + α 因子选股）  
> 仅依赖免费 **TuShare Pro** 数据，适合 5 – 50 k 量级账户快速实践。

---

## Quick Start

```bash
# 1. 拉仓库
git clone https://github.com/fish-wjj/mini-alpha-lite.git
cd mini-alpha-lite

# 2. 环境
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 3. 配置 TuShare Token
cp .env.example .env            # 编辑填入 TUSHARE_TOKEN=xxxxxxxxx

# 4. 生成订单
python -m src.gen_orders        # → orders_YYYYMMDD.csv
