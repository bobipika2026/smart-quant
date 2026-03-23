#!/usr/bin/env python
"""
AkShare小时线数据同步

优点：免费、无限频
"""
import os
import sys
import time
import sqlite3
import pandas as pd
import akshare as ak
from datetime import datetime

sys.path.insert(0, '.')

CACHE_DIR = "data_cache/hour"
os.makedirs(CACHE_DIR, exist_ok=True)

print("=" * 60)
print("AkShare小时线数据同步")
print("=" * 60)

# 获取缺失的股票代码
def get_missing_codes():
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stocks WHERE exchange IN ('SH', 'SZ')")
    all_codes = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    existing = set(f.replace('_60min.csv', '') for f in os.listdir(CACHE_DIR) if f.endswith('_60min.csv'))
    missing = list(all_codes - existing)
    return missing

# 同步单只股票
def sync_hour_data(code: str) -> bool:
    """从AkShare同步小时线数据"""
    symbol = f'sh{code}' if code.startswith(('6', '9')) else f'sz{code}'
    
    try:
        df = ak.stock_zh_a_minute(symbol=symbol, period='60', adjust='qfq')
        
        if df is not None and len(df) > 0:
            # 重命名列
            df = df.rename(columns={
                'day': '时间',
                'open': '开盘',
                'high': '最高',
                'low': '最低',
                'close': '收盘',
                'volume': '成交量',
                'amount': '成交额'
            })
            
            # 按时间排序
            df = df.sort_values('时间').reset_index(drop=True)
            
            # 保存
            cache_file = os.path.join(CACHE_DIR, f"{code}_60min.csv")
            df.to_csv(cache_file, index=False)
            return True
        
        return False
        
    except Exception as e:
        return False

# 主程序
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--batch', type=int, default=100, help='每批数量')
    parser.add_argument('--limit', type=int, default=0, help='限制数量，0为全部')
    args = parser.parse_args()
    
    # 获取缺失代码
    missing_codes = get_missing_codes()
    print(f"\n缺失小时线数据: {len(missing_codes)} 只")
    
    if args.limit > 0:
        missing_codes = missing_codes[:args.limit]
        print(f"限制同步: {len(missing_codes)} 只")
    
    if not missing_codes:
        print("所有数据已存在！")
        sys.exit(0)
    
    # 开始同步
    print("\n开始同步...")
    success, fail = 0, 0
    start_time = time.time()
    
    for i, code in enumerate(missing_codes):
        result = sync_hour_data(code)
        
        if result:
            success += 1
        else:
            fail += 1
        
        # 进度
        if (i + 1) % args.batch == 0 or i == len(missing_codes) - 1:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(missing_codes) - i - 1) / rate / 60 if rate > 0 else 0
            
            print(
                f"进度: {i+1}/{len(missing_codes)} | "
                f"成功: {success} | 失败: {fail} | "
                f"速度: {rate:.1f}只/秒 | "
                f"剩余: {eta:.1f}分钟"
            )
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"同步完成: 成功 {success}, 失败 {fail}")
    print(f"耗时: {elapsed/60:.1f}分钟")
    print("=" * 60)