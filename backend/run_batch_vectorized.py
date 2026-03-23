#!/usr/bin/env python3
"""批量向量化回测"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_vectorized_backtest import run_vectorized_factor_matrix
import sqlite3
from datetime import datetime

def get_stocks():
    with open('filtered_stocks.txt', 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_cached_stocks():
    cached = set()
    for f in os.listdir('data_cache/day'):
        if f.endswith('_day.csv'):
            cached.add(f.replace('_day.csv', ''))
    return cached

def get_processed_stocks():
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code FROM best_factor_combinations WHERE is_active = 1")
    processed = set(row[0] for row in cursor.fetchall())
    conn.close()
    return processed

print("=" * 60)
print("批量向量化回测 (与回测引擎100%一致)")
print("=" * 60)

cached = get_cached_stocks()
processed = get_processed_stocks()
pending = [s for s in get_stocks() if s in cached and s not in processed]

print(f"已缓存: {len(cached)}, 已回测: {len(processed)}, 待回测: {len(pending)}")
print(f"开始时间: {datetime.now()}")

success, failed = 0, 0

for i, code in enumerate(pending):
    print(f"\n[{i+1}/{len(pending)}] {code}")
    
    try:
        result = run_vectorized_factor_matrix(code, top_n=10)
        
        if result.get('total_experiments', 0) > 0:
            success += 1
            top = result.get('top_results', [])
            if top:
                print(f"  ✅ Top收益: {top[0]['total_return']:.1f}%")
        else:
            failed += 1
            print("  ⚠️ 无有效结果")
            
    except Exception as e:
        failed += 1
        print(f"  ❌ {e}")

print(f"\n{'='*60}")
print(f"完成! 成功: {success}, 失败: {failed}")
print(f"结束时间: {datetime.now()}")
print("=" * 60)
