#!/usr/bin/env bash
# 放到项目根目录，确保 chmod +x auto-sync.sh
# 运行： ./auto-sync.sh     （建议用 screen 或 tmux 挂后台）

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_DIR" || exit 1

BRANCH="main"        # 或其他分支名
INTERVAL=2           # 两秒内批量归并事件，避免频繁 push
LOGFILE="$REPO_DIR/auto-sync.log"

echo "🔄 Auto-sync started in $REPO_DIR  →  branch: $BRANCH" | tee -a "$LOGFILE"

while true; do
  # 监听写入 / 移动 / 删除事件，排除 .git 目录自身
  inotifywait -qr -e modify,create,delete,move --exclude '\.git/' "$REPO_DIR"
  sleep "$INTERVAL"

  # 若工作区有变动
  if ! git -C "$REPO_DIR" diff --quiet; then
    TS=$(date '+%F %T')
    git -C "$REPO_DIR" add -A
    git -C "$REPO_DIR" commit -m "auto-sync: $TS" --author="auto-bot <>" \
      && git -C "$REPO_DIR" pull --rebase \
      && git -C "$REPO_DIR" push origin "$BRANCH"
    echo "[$TS] synced ✔" | tee -a "$LOGFILE"
  fi
done
