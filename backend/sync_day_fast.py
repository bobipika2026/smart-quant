#!/usr/bin/env python
"""
日线数据增量更新 - 5000积分版

Tushare 5000积分用户: 每分钟50次请求
"""
import os
import sys
import time
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
from collections import Counter

sys.path.insert(0, '.')
from app.config import settings

DAY_DIR = "data_cache/day"
os.makedirs(DAY_DIR, exist_ok=True)

# 5000积分用户：每分钟50次
RATE_LIMIT = 50  # 每分钟请求数
INTERVAL = 60 / RATE_LIMIT  # 每次请求间隔（秒）≈1.2秒

if settings.TUSHARE_TOKEN:
    ts.set_token(settings.TUSHARE_TOKEN)

print("=" * 60)
print("日线数据增量更新 (5000积分版)")
print(f"API限频: 每分钟 {RATE_LIMIT} 次")
print("=" * 60)

# 获取需要更新的股票
def get_outdated_stocks(target_date='20260319'):
    """获取数据过期的股票"""
    files = [f for f in os.listdir(DAY_DIR) if f.endswith('_day.csv')]
    outdated = []
    
    for f in files:
        try:
            df = pd.read_csv(os.path.join(DAY_DIR, f))
            if len(df) > 0 and '日期' in df.columns:
                last_date = str(df['日期'].iloc[-1])
                if last_date < target_date:
                    code = f.replace('_day.csv', '')
                    outdated.append((code, last_date))
        except:
            pass
    
    return outdated

# 批量更新
def batch_update(codes, batch_size=100):
    """批量更新日线数据"""
    success, fail = 0, 0
    start_time = time.time()
    
    for i, (code, old_date) in enumerate(codes):
        ts_code = f"{code}.SZ" if code.startswith(('0', '3')) else f"{code}.SH"
        
        try:
            # 获取最新数据
            df = ts.pro_bar(ts_code=ts_code, start_date='20260301', end_date='20260320', adj='qfq')
            
            if df is not None and len(df) > 0:
                # 读取现有数据
                cache_file = os.path.join(DAY_DIR, f"{code}_day.csv")
                existing = pd.read_csv(cache_file)
                
                # 重命名列
                df = df.rename(columns={
                    'trade_date': '日期',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'close': '收盘',
                    'vol': '成交量',
                    'amount': '成交额',
                    'pct_chg': '涨跌幅'
                })
                
                # 转换日期格式
                if '日期' in existing.columns:
                    existing['日期'] = existing['日期'].astype(str).str.replace('-', '')
                
                # 合并去重
                combined = pd.concat([existing, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['日期'], keep='last')
                combined = combined.sort_values('日期').reset_index(drop=True)
                
                # 保存
                combined.to_csv(cache_file, index=False)
                success += 1
            else:
                fail += 1
                
        except Exception as e:
            fail += 1
        
        # 限频控制（5000积分：每分钟50次 ≈ 1.2秒/次）
        time.sleep(INTERVAL)
        
        # 进度报告
        if (i + 1) % batch_size == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            eta = (len(codes) - i - 1) / rate / 60
            print(f"进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail} | 速度: {rate:.1f}只/秒 | 剩余: {eta:.1f}分钟")
    
    return success, fail

# 主程序
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--target', default='20260319', help='目标日期')
    args = parser.parse_args()
    
    # 获取过期股票
    outdated = get_outdated_stocks(args.target)
    print(f"\n需要更新: {len(outdated)} 只股票")
    
    if not outdated:
        print("所有数据已是最新！")
        sys.exit(0)
    
    # 显示示例
    print("\n示例:")
    for code, date in outdated[:5]:
        print(f"  {code}: {date}")
    
    # 开始更新
    print("\n开始更新...")
    success, fail = batch_update(outdated)
    
    print("\n" + "=" * 60)
    print(f"更新完成: 成功 {success}, 失败 {fail}")
    print("=" * 60)