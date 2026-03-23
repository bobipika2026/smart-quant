#!/usr/bin/env python
"""
Tushare分钟线数据同步（限频版）

API限制：每分钟最多2次请求
"""
import os
import sys
import time
import asyncio
import logging
from datetime import datetime, timedelta
import sqlite3

sys.path.insert(0, '.')

import tushare as ts
import pandas as pd
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/sync_hour_tushare.log')
    ]
)
logger = logging.getLogger(__name__)

# 配置
CACHE_DIR = "data_cache/hour"
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Tushare限频：每分钟最多2次
RATE_LIMIT = 2  # 每分钟请求数
INTERVAL = 60 / RATE_LIMIT  # 每次请求间隔（秒）

if settings.TUSHARE_TOKEN:
    ts.set_token(settings.TUSHARE_TOKEN)


def get_missing_codes():
    """获取缺失小时线的股票代码"""
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stocks WHERE exchange IN ('SH', 'SZ')")
    all_codes = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    existing = set(f.replace('_60min.csv', '') for f in os.listdir(CACHE_DIR) if f.endswith('_60min.csv'))
    missing = list(all_codes - existing)
    logger.info(f"缺失小时线数据: {len(missing)} 只")
    return missing


def sync_hour_data_tushare(code: str, years: int = 2) -> bool:
    """
    使用Tushare同步小时线数据
    
    Args:
        code: 股票代码
        years: 历史年数
    
    Returns:
        是否成功
    """
    ts_code = f"{code}.SZ" if code.startswith(('0', '3')) else f"{code}.SH"
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = ts.pro_bar(ts_code=ts_code, freq='60min', start_date=start_date, end_date=end_date)
        
        if df is not None and len(df) > 0:
            # 重命名列
            df = df.rename(columns={
                'trade_time': '时间',
                'open': '开盘',
                'close': '收盘',
                'high': '最高',
                'low': '最低',
                'vol': '成交量',
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
        logger.debug(f"{code} 失败: {e}")
        return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Tushare小时线同步')
    parser.add_argument('--limit', type=int, default=0, help='限制同步数量，0为全部')
    parser.add_argument('--batch', type=int, default=100, help='每批数量')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Tushare小时线数据同步")
    logger.info(f"API限频: 每分钟{RATE_LIMIT}次")
    logger.info("=" * 60)
    
    # 获取缺失代码
    missing_codes = get_missing_codes()
    
    if args.limit > 0:
        missing_codes = missing_codes[:args.limit]
        logger.info(f"限制同步: {len(missing_codes)} 只")
    
    if not missing_codes:
        logger.info("没有缺失数据")
        return
    
    total = len(missing_codes)
    success, fail = 0, 0
    start_time = time.time()
    
    for i, code in enumerate(missing_codes):
        # 限频控制
        if i > 0 and i % RATE_LIMIT == 0:
            time.sleep(INTERVAL)
        
        # 同步
        result = sync_hour_data_tushare(code)
        
        if result:
            success += 1
        else:
            fail += 1
        
        # 进度
        if (i + 1) % args.batch == 0 or i == total - 1:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            
            logger.info(
                f"进度: {i+1}/{total} | "
                f"成功: {success} | 失败: {fail} | "
                f"速度: {rate:.1f}只/秒 | "
                f"预计剩余: {eta/60:.1f}分钟"
            )
    
    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"同步完成: 成功 {success}, 失败 {fail}")
    logger.info(f"耗时: {elapsed/60:.1f}分钟")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()