#!/usr/bin/env bash
# —— 自动把本地改动同步到远程 —— #
set -euo pipefail

WORKDIR="/mini-alpha-lite"
cd "$WORKDIR"

# 确保存在 main 分支
git symbolic-ref HEAD refs/heads/main 2>/dev/null || true

echo "[auto-sync] watching $WORKDIR ..."
# 持续监听：新增、修改、删除、移动 4 类事件
while inotifywait -r -e modify,create,delete,move --exclude '\.git/' .; do
  # 暂存所有改动（含删除）
  git add -A

  # 如果没有新内容就跳过
  if git diff --cached --quiet; then
    continue
  fi

  MSG="auto: $(date '+%F %T')"
  git commit -m "$MSG" --author="sync-bot <sync@localhost>"
  # 网络偶发失败也不退出
  git push origin main || echo "[auto-sync] push failed, will retry on next change"
done
