#!/usr/bin/env bash
# 每日采集入口：先体检历史数据，不合格就不发布。
# 用法: bash scripts/daily.sh
set -uo pipefail
cd "$(dirname "$0")/.."

echo "=== 1/3 历史数据体检 + 自动回填 ==="
python3 scripts/guard_history.py
code=$?
if [ "$code" -ne 0 ]; then
  echo "!! 历史数据仍有缺口（退出码 $code），中止本次发布。明细见 data/_quality.json"
  exit 2
fi

echo "=== 2/3 采集行情与因子 ==="
python3 collect.py || { echo "!! collect.py 失败"; exit 3; }

echo "=== 3/3 发布产物检查 ==="
python3 scripts/guard_history.py --dry >/dev/null && echo "✅ 全部通过" || echo "⚠ 发布后体检异常，请查 data/_quality.json"
