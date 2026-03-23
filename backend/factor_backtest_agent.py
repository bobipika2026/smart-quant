#!/usr/bin/env python3
"""
因子回测Agent - 使用向量化引擎高性能回测
速度: 15000+ 实验/秒
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
CHECK_INTERVAL = 30
TOP_N = 100

def get_filtered_stocks():
    stocks_file = 'filtered_stocks.txt'
    if not os.path.exists(stocks_file):
        return []
    with open(stocks_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_cached_stocks():
    cache_dir = "data_cache/day"
    if not os.path.exists(cache_dir):
        return set()
    cached = set()
    for f in os.listdir(cache_dir):
        if f.endswith("_day.csv"):
            cached.add(f.replace("_day.csv", ""))
    return cached

def get_backtested_stocks():
    try:
        conn = sqlite3.connect('smart_quant.db')
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT stock_code FROM best_factor_combinations WHERE is_active = 1")
        backtested = set(row[0] for row in cursor.fetchall())
        conn.close()
        return backtested
    except:
        return set()

def run_backtest_sync(code):
    """同步执行向量化回测（run_vectorized_factor_matrix 内部已保存）"""
    from run_vectorized_backtest import run_vectorized_factor_matrix
    
    try:
        result = run_vectorized_factor_matrix(
            stock_code=code,
            top_n=TOP_N
        )
        
        if 'error' in result:
            return False, result.get('error', '未知错误')
        
        top_results = result.get('top_results', [])
        
        if not top_results:
            return False, "无有效结果"
        
        # 找最佳收益
        best = max(r.get('total_return', 0) for r in top_results)
        
        if best <= 0:
            return False, "无正收益策略"
        
        return True, best
        
    except Exception as e:
        return False, str(e)

async def run_vectorized_backtest(code):
    """异步包装"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, run_backtest_sync, code)

async def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("因子回测Agent (向量化引擎)")
    print("速度: 15000+ 实验/秒")
    print("=" * 60)
    
    all_stocks = get_filtered_stocks()
    print(f"筛选后股票: {len(all_stocks)}")
    
    total_success, total_failed = 0, 0
    best_returns = []
    
    while True:
        cached_stocks = get_cached_stocks()
        backtested_stocks = get_backtested_stocks()
        
        pending = [s for s in all_stocks if s in cached_stocks and s not in backtested_stocks]
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 已缓存: {len(cached_stocks)}, 已回测: {len(backtested_stocks)}, 待回测: {len(pending)}")
        
        if not pending:
            uncached = [s for s in all_stocks if s not in cached_stocks]
            if not uncached:
                print("\n✅ 所有股票已回测完成!")
                break
            print(f"等待数据同步... ({len(uncached)} 只未缓存)")
            await asyncio.sleep(CHECK_INTERVAL)
            continue
        
        batch_size = 20
        batch = pending[:batch_size]
        
        print(f"回测 {len(batch)} 只股票...")
        
        for code in batch:
            success, result = await run_vectorized_backtest(code)
            
            if success:
                total_success += 1
                best_returns.append({'code': code, 'return': result})
                print(f"  {code}: ✅ {result:.1f}%")
            else:
                total_failed += 1
                print(f"  {code}: ⚠️ {result}")
        
        await asyncio.sleep(1)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    if best_returns:
        best_returns.sort(key=lambda x: x['return'], reverse=True)
        print(f"\n{'='*60}")
        print("Top 20 最佳收益:")
        for i, item in enumerate(best_returns[:20]):
            print(f"  #{i+1:2d} {item['code']}: {item['return']:.1f}%")
    
    print(f"\n{'='*60}")
    print(f"因子回测完成!")
    print(f"成功: {total_success}, 失败: {total_failed}")
    print(f"耗时: {duration/60:.1f} 分钟")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())