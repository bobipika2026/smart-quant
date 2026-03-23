#!/bin/bash
# 数据补充任务 - 顺序执行所有同步任务

cd /Users/hmh/.openclaw/workspace-pmofinclaw_agent/smart-quant/backend

echo "=========================================="
echo "数据补充任务开始 - $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# 创建日志目录
mkdir -p logs

# 1. 大盘数据（最快）
echo ""
echo "[1/4] 同步大盘数据..."
python3 fill_missing_data.py index

# 2. 日线数据
echo ""
echo "[2/4] 同步日线数据..."
python3 fill_missing_data.py day

# 3. 小时线数据
echo ""
echo "[3/4] 同步小时线数据..."
python3 fill_missing_data.py hour

# 4. 财务数据
echo ""
echo "[4/4] 同步财务数据..."
python3 fill_missing_data.py financial

echo ""
echo "=========================================="
echo "数据补充任务完成 - $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# 显示最终统计
echo ""
echo "数据统计:"
echo "  日线数据: $(ls data_cache/day/*.csv 2>/dev/null | wc -l | tr -d ' ') 只"
echo "  小时线: $(ls data_cache/hour/*.csv 2>/dev/null | wc -l | tr -d ' ') 只"
echo "  1分钟: $(ls data_cache/minute/*.csv 2>/dev/null | wc -l | tr -d ' ') 只"
echo "  财务数据: $(ls data_cache/financial/*.csv 2>/dev/null | wc -l | tr -d ' ') 个文件"
echo "  大盘指数: $(ls data_cache/index/*.csv 2>/dev/null | wc -l | tr -d ' ') 个"