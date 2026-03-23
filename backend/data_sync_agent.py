#!/usr/bin/env python3
"""
数据同步Agent - 专门负责同步日线和60分钟线数据
"""
import os
import sys
import asyncio
from datetime import datetime
import functools

print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.data_sync import DataSyncService

# 配置
BATCH_SIZE = 50
SYNC_DAY = True
SYNC_HOUR = True

def get_filtered_stocks():
    """读取筛选后的股票列表"""
    stocks_file = 'filtered_stocks.txt'
    if not os.path.exists(stocks_file):
        print(f"❌ 未找到 {stocks_file}")
        return []
    with open(stocks_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

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

async def sync_batch(service, codes, batch_num, total_batches):
    """同步一批股票"""
    print(f"\n{'='*60}")
    print(f"[数据同步] 批次 {batch_num}/{total_batches}: {len(codes)} 只")
    print(f"{'='*60}")
    
    success, failed = 0, 0
    
    for i, code in enumerate(codes):
        try:
            print(f"[{i+1}/{len(codes)}] {code}", end=" ")
            
            if SYNC_DAY:
                df_day = await service.sync_day_data_tushare(code, years=10)
                await asyncio.sleep(0.3)
            
            if SYNC_HOUR:
                df_hour = await service.sync_hour_data_akshare(code)
            
            if len(df_day if SYNC_DAY else df_hour) > 0:
                success += 1
                print(f"✅ 日线{len(df_day) if SYNC_DAY else 'skip'}条")
            else:
                failed += 1
                print("⚠️ 无数据")
                
        except Exception as e:
            failed += 1
            print(f"❌ {e}")
    
    return success, failed

async def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("数据同步Agent")
    print("条件: 非ST + 市值>=50亿")
    print("=" * 60)
    
    all_stocks = get_filtered_stocks()
    cached_stocks = get_cached_stocks()
    pending_stocks = [s for s in all_stocks if s not in cached_stocks]
    
    print(f"\n总数: {len(all_stocks)}, 已缓存: {len(cached_stocks)}, 待同步: {len(pending_stocks)}")
    
    if not pending_stocks:
        print("✅ 全部已缓存")
        return
    
    service = DataSyncService()
    total_batches = (len(pending_stocks) + BATCH_SIZE - 1) // BATCH_SIZE
    
    total_success, total_failed = 0, 0
    
    for batch_num in range(total_batches):
        start_idx = batch_num * BATCH_SIZE
        end_idx = min(start_idx + BATCH_SIZE, len(pending_stocks))
        batch_codes = pending_stocks[start_idx:end_idx]
        
        success, failed = await sync_batch(service, batch_codes, batch_num + 1, total_batches)
        total_success += success
        total_failed += failed
        
        # 更新进度文件
        with open('sync_progress.txt', 'w') as f:
            cached_now = len(get_cached_stocks())
            f.write(f"{datetime.now()}\n{cached_now}/{len(all_stocks)}\n批次{batch_num+1}/{total_batches}\n")
        
        if batch_num < total_batches - 1:
            print(f"\n休息5秒...")
            await asyncio.sleep(5)
    
    duration = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*60}")
    print(f"数据同步完成!")
    print(f"成功: {total_success}, 失败: {total_failed}")
    print(f"耗时: {duration/60:.1f} 分钟")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())