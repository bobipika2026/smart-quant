#!/usr/bin/env python3
"""
因子回测Agent - 使用正确的回测引擎
修复向量化引擎的数据错误
"""
import os
import sys
import asyncio
import sqlite3
from datetime import datetime
import functools

print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 配置
CHECK_INTERVAL = 10
MAX_EXPERIMENTS = 50

def get_stocks_to_backtest():
    """获取需要重新回测的股票列表"""
    # 从筛选列表中排除已有正确数据的股票
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT stock_code FROM best_factor_combinations WHERE is_active = 1")
    existing = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    with open('filtered_stocks.txt', 'r') as f:
        all_stocks = [line.strip() for line in f if line.strip()]
    
    return [s for s in all_stocks if s not in existing]

def get_cached_stocks():
    """获取已缓存日线的股票"""
    cache_dir = "data_cache/day"
    if not os.path.exists(cache_dir):
        return set()
    cached = set()
    for f in os.listdir(cache_dir):
        if f.endswith("_day.csv"):
            cached.add(f.replace("_day.csv", ""))
    return cached

async def run_backtest(code):
    """使用正确的回测引擎"""
    from app.services.factor_matrix_v2_service import FactorMatrixV2
    
    try:
        result = await FactorMatrixV2.run_batch_experiments(
            stock_code=code,
            max_experiments=MAX_EXPERIMENTS
        )
        
        successful = result.get('successful', 0)
        if successful > 0:
            results = result.get('results', [])
            positive = [r for r in results if r.get('total_return', 0) > 0]
            if positive:
                best = max(r.get('total_return', 0) for r in positive)
                return True, best
        
        return False, "无有效策略"
        
    except Exception as e:
        return False, str(e)

async def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("因子回测Agent (正确引擎)")
    print("修复向量化引擎数据错误")
    print("=" * 60)
    
    # 获取需要回测的股票
    pending = get_stocks_to_backtest()
    cached = get_cached_stocks()
    pending = [s for s in pending if s in cached]
    
    print(f"需要回测: {len(pending)} 只股票")
    
    if not pending:
        print("✅ 所有股票已有正确数据")
        return
    
    success, failed = 0, 0
    best_returns = []
    
    for i, code in enumerate(pending):
        print(f"\n[{i+1}/{len(pending)}] {code}")
        
        ok, result = await run_backtest(code)
        
        if ok:
            success += 1
            best_returns.append({'code': code, 'return': result})
            print(f"  ✅ 最佳收益: {result:.1f}%")
        else:
            failed += 1
            print(f"  ⚠️ {result}")
        
        # 避免过载
        await asyncio.sleep(0.5)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    if best_returns:
        best_returns.sort(key=lambda x: x['return'], reverse=True)
        print(f"\n{'='*60}")
        print("Top 10 最佳收益:")
        for i, item in enumerate(best_returns[:10]):
            print(f"  #{i+1} {item['code']}: {item['return']:.1f}%")
    
    print(f"\n{'='*60}")
    print(f"回测完成!")
    print(f"成功: {success}, 失败: {failed}")
    print(f"耗时: {duration/60:.1f} 分钟")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())