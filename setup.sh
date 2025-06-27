python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
touch db.sqlite
echo "初始化完成 ✅"