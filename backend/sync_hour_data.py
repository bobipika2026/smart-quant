#!/usr/bin/env python3
"""
补充60分钟线数据
"""
import os
import sys
import asyncio
from datetime import datetime
import functools

print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.data_sync import DataSyncService

async def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("补充60分钟线数据")
    print("=" * 60)
    
    # 读取缺少60分钟数据的股票列表
    with open('/tmp/missing_hour.txt', 'r') as f:
        missing_codes = [line.strip() for line in f if line.strip()]
    
    print(f"缺少60分钟数据的股票: {len(missing_codes)}")
    
    service = DataSyncService()
    success, failed = 0, 0
    
    for i, code in enumerate(missing_codes):
        try:
            print(f"[{i+1}/{len(missing_codes)}] {code} ", end="")
            df = await service.sync_hour_data_akshare(code)
            
            if len(df) > 0:
                success += 1
                print(f"✅ {len(df)}条")
            else:
                failed += 1
                print("⚠️ 无数据")
            
            # 避免请求过快
            await asyncio.sleep(0.5)
            
        except Exception as e:
            failed += 1
            print(f"❌ {e}")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print(f"60分钟线同步完成!")
    print(f"成功: {success}, 失败: {failed}")
    print(f"耗时: {duration/60:.1f} 分钟")
    print("=" * 60)

if __name__ == '__main__':
    asyncio.run(main())