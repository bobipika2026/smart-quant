#!/usr/bin/env python3
"""
批量数据同步 + 因子矩阵回测
仅处理符合条件的股票（非ST + 市值>=50亿）
"""
import os
import sys
import asyncio
import sqlite3
from datetime import datetime
import functools

# 实时输出
print = functools.partial(print, flush=True)

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.data_sync import DataSyncService
from app.services.factor_matrix_v2_service import FactorMatrixV2

# 配置
BATCH_SIZE = 50  # 每批处理股票数
SYNC_DAY = True
SYNC_HOUR = True
SYNC_MINUTE = False  # 1分钟数据量大，可选
RUN_BACKTEST = True

def get_filtered_stocks():
    """读取筛选后的股票列表"""
    stocks_file = 'filtered_stocks.txt'
    if not os.path.exists(stocks_file):
        print(f"❌ 未找到 {stocks_file}，请先运行 filter_stocks_akshare.py")
        return []
    
    with open(stocks_file, 'r') as f:
        codes = [line.strip() for line in f if line.strip()]
    return codes

def get_cached_stocks():
    """获取已缓存日线的股票"""
    cache_dir = "data_cache/day"
    if not os.path.exists(cache_dir):
        return set()
    
    cached = set()
    for f in os.listdir(cache_dir):
        if f.endswith("_day.csv"):
            code = f.replace("_day.csv", "")
            cached.add(code)
    return cached

async def sync_batch(service, codes, batch_num, total_batches):
    """同步一批股票"""
    print(f"\n{'='*60}")
    print(f"批次 {batch_num}/{total_batches}: 同步 {len(codes)} 只股票")
    print(f"{'='*60}")
    
    results = {
        'success': 0,
        'failed': 0,
        'details': {}
    }
    
    for i, code in enumerate(codes):
        try:
            print(f"\n[{i+1}/{len(codes)}] {code}")
            
            # 日线
            if SYNC_DAY:
                df_day = await service.sync_day_data_tushare(code, years=10)
                await asyncio.sleep(0.5)  # 避免Tushare限频
            
            # 60分钟
            if SYNC_HOUR:
                df_hour = await service.sync_hour_data_akshare(code)
            
            # 1分钟（可选，数据量大）
            if SYNC_MINUTE:
                df_min = await service.sync_minute_data_akshare(code)
            
            results['success'] += 1
            results['details'][code] = 'success'
            
        except Exception as e:
            print(f"  ❌ 失败: {e}")
            results['failed'] += 1
            results['details'][code] = str(e)
    
    return results

async def run_factor_backtest(codes):
    """运行因子矩阵回测"""
    print(f"\n{'='*60}")
    print(f"因子矩阵回测: {len(codes)} 只股票")
    print(f"{'='*60}")
    
    results = {
        'total': len(codes),
        'success': 0,
        'failed': 0,
        'best_returns': []
    }
    
    for i, code in enumerate(codes):
        try:
            print(f"\n[{i+1}/{len(codes)}] {code} 因子回测...")
            
            # 运行批量实验
            result = await FactorMatrixV2.run_batch_experiments(
                stock_code=code,
                max_experiments=50  # 每只股票50个实验
            )
            
            if 'error' not in result:
                results['success'] += 1
                best_return = result.get('best_return')
                if best_return:
                    results['best_returns'].append({
                        'code': code,
                        'return': best_return
                    })
            else:
                results['failed'] += 1
                
        except Exception as e:
            print(f"  ❌ 回测失败: {e}")
            results['failed'] += 1
    
    # 排序最佳收益
    results['best_returns'].sort(key=lambda x: x['return'], reverse=True)
    
    return results

async def main():
    """主函数"""
    start_time = datetime.now()
    
    print("=" * 60)
    print("批量数据同步 + 因子矩阵回测")
    print("筛选条件: 非ST + 市值>=50亿")
    print("=" * 60)
    print(f"开始时间: {start_time}")
    
    # 获取筛选后的股票列表
    all_stocks = get_filtered_stocks()
    if not all_stocks:
        return
    
    cached_stocks = get_cached_stocks()
    
    # 找出未缓存的股票
    pending_stocks = [s for s in all_stocks if s not in cached_stocks]
    
    print(f"\n筛选后股票总数: {len(all_stocks)}")
    print(f"已缓存: {len(cached_stocks)}")
    print(f"待同步: {len(pending_stocks)}")
    
    if not pending_stocks:
        print("\n✅ 所有股票已缓存，跳过同步")
    else:
        # 分批同步
        service = DataSyncService()
        total_batches = (len(pending_stocks) + BATCH_SIZE - 1) // BATCH_SIZE
        
        sync_results = []
        for batch_num in range(total_batches):
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, len(pending_stocks))
            batch_codes = pending_stocks[start_idx:end_idx]
            
            result = await sync_batch(service, batch_codes, batch_num + 1, total_batches)
            sync_results.append(result)
            
            # 批次间隔
            if batch_num < total_batches - 1:
                print(f"\n批次完成，休息10秒...")
                await asyncio.sleep(10)
        
        # 汇总同步结果
        total_success = sum(r['success'] for r in sync_results)
        total_failed = sum(r['failed'] for r in sync_results)
        print(f"\n{'='*60}")
        print(f"数据同步完成")
        print(f"成功: {total_success}, 失败: {total_failed}")
    
    # 运行因子回测
    if RUN_BACKTEST:
        # 获取所有已缓存的股票
        all_cached = get_cached_stocks()
        # 只回测筛选列表中的股票
        to_backtest = [s for s in all_stocks if s in all_cached]
        backtest_result = await run_factor_backtest(to_backtest)
        
        print(f"\n{'='*60}")
        print(f"因子回测完成")
        print(f"成功: {backtest_result['success']}, 失败: {backtest_result['failed']}")
        
        # 显示Top 10最佳收益
        if backtest_result['best_returns']:
            print(f"\nTop 10 最佳收益:")
            for i, item in enumerate(backtest_result['best_returns'][:10]):
                print(f"  #{i+1} {item['code']}: {item['return']:.1f}%")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print(f"全部完成!")
    print(f"总耗时: {duration/60:.1f} 分钟")
    print(f"结束时间: {end_time}")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())