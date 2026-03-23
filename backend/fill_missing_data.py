#!/usr/bin/env python
"""
数据补齐脚本

根据缺失报告，补齐缺失的数据
"""
import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tushare as ts
import akshare as ak

from app.config import settings
from app.services.data_sync import DataSyncService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/fill_data.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# 缓存目录
CACHE_DIRS = {
    'day': 'data_cache/day',
    'hour': 'data_cache/hour',
    'minute': 'data_cache/minute',
    'financial': 'data_cache/financial',
    'index': 'data_cache/index',
}

# 大盘指数
INDEX_CODES = {
    '000001.SH': '上证指数',
    '399001.SZ': '深证成指',
    '399006.SZ': '创业板指',
    '000016.SH': '上证50',
    '000300.SH': '沪深300',
    '000905.SH': '中证500',
    '000852.SH': '中证1000',
}


def get_missing_codes(data_type: str) -> Set[str]:
    """获取缺失的股票代码"""
    import sqlite3
    
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stocks WHERE exchange IN ('SH', 'SZ')")
    all_codes = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    cache_dir = CACHE_DIRS[data_type]
    
    if data_type == 'day':
        suffix = '_day.csv'
    elif data_type == 'hour':
        suffix = '_60min.csv'
    elif data_type == 'minute':
        suffix = '_1min.csv'
    elif data_type == 'financial':
        # 财务数据需要检查两个文件
        fina_files = set(f.replace('_fina_indicator.csv', '') 
                        for f in os.listdir(cache_dir) if f.endswith('_fina_indicator.csv'))
        basic_files = set(f.replace('_daily_basic.csv', '') 
                         for f in os.listdir(cache_dir) if f.endswith('_daily_basic.csv'))
        existing = fina_files & basic_files
        return all_codes - existing
    else:
        return set()
    
    if os.path.exists(cache_dir):
        existing = set(f.replace(suffix, '') for f in os.listdir(cache_dir) if f.endswith(suffix))
    else:
        existing = set()
    
    return all_codes - existing


async def sync_index_data():
    """同步大盘指数数据"""
    logger.info("=" * 60)
    logger.info("开始同步大盘指数数据")
    logger.info("=" * 60)
    
    os.makedirs(CACHE_DIRS['index'], exist_ok=True)
    
    if settings.TUSHARE_TOKEN:
        ts.set_token(settings.TUSHARE_TOKEN)
    
    pro = ts.pro_api()
    
    for ts_code, name in INDEX_CODES.items():
        try:
            logger.info(f"同步 {name} ({ts_code})...")
            
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y%m%d')
            
            df = pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if df is not None and len(df) > 0:
                df = df.rename(columns={
                    'trade_date': '日期',
                    'open': '开盘',
                    'close': '收盘',
                    'high': '最高',
                    'low': '最低',
                    'vol': '成交量',
                    'amount': '成交额',
                    'pct_chg': '涨跌幅'
                })
                df = df.sort_values('日期').reset_index(drop=True)
                
                code = ts_code.split('.')[0]
                cache_file = os.path.join(CACHE_DIRS['index'], f"{code}_index.csv")
                df.to_csv(cache_file, index=False)
                
                logger.info(f"  ✓ {name}: {len(df)}条")
            else:
                logger.warning(f"  ✗ {name}: 无数据")
            
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"  ✗ {name}: {e}")
    
    logger.info("大盘指数数据同步完成")


async def sync_day_data(codes: List[str], batch_size: int = 50):
    """同步日线数据"""
    logger.info("=" * 60)
    logger.info(f"开始同步日线数据，共 {len(codes)} 只股票")
    logger.info("=" * 60)
    
    sync_service = DataSyncService()
    success, fail = 0, 0
    
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        logger.info(f"处理批次 {i//batch_size + 1}/{(len(codes)+batch_size-1)//batch_size}")
        
        for code in batch:
            try:
                df = await sync_service.sync_day_data_tushare(code, years=5)
                if len(df) > 0:
                    success += 1
                else:
                    fail += 1
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"{code} 失败: {e}")
                fail += 1
        
        if i + batch_size < len(codes):
            logger.info("等待10秒...")
            await asyncio.sleep(10)
    
    logger.info(f"日线同步完成: 成功 {success}, 失败 {fail}")


async def sync_hour_data(codes: List[str], batch_size: int = 50):
    """同步小时线数据"""
    logger.info("=" * 60)
    logger.info(f"开始同步小时线数据，共 {len(codes)} 只股票")
    logger.info("=" * 60)
    
    sync_service = DataSyncService()
    success, fail = 0, 0
    
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        logger.info(f"处理批次 {i//batch_size + 1}/{(len(codes)+batch_size-1)//batch_size}")
        
        for code in batch:
            try:
                df = await sync_service.sync_hour_data_akshare(code)
                if len(df) > 0:
                    success += 1
                else:
                    fail += 1
            except Exception as e:
                logger.debug(f"{code} 失败: {e}")
                fail += 1
        
        if i + batch_size < len(codes):
            logger.info("等待10秒...")
            await asyncio.sleep(10)
    
    logger.info(f"小时线同步完成: 成功 {success}, 失败 {fail}")


async def sync_financial_data(codes: List[str], batch_size: int = 50):
    """同步财务数据"""
    logger.info("=" * 60)
    logger.info(f"开始同步财务数据，共 {len(codes)} 只股票")
    logger.info("=" * 60)
    
    if settings.TUSHARE_TOKEN:
        ts.set_token(settings.TUSHARE_TOKEN)
    
    pro = ts.pro_api()
    success, fail = 0, 0
    
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        logger.info(f"处理批次 {i//batch_size + 1}/{(len(codes)+batch_size-1)//batch_size}")
        
        for code in batch:
            try:
                ts_code = f"{code}.SZ" if code.startswith(('0', '3')) else f"{code}.SH"
                
                # fina_indicator
                df_fina = pro.fina_indicator(ts_code=ts_code, start_date='20200101')
                if df_fina is not None and len(df_fina) > 0:
                    cache_file = os.path.join(CACHE_DIRS['financial'], f"{code}_fina_indicator.csv")
                    df_fina.to_csv(cache_file, index=False)
                
                # daily_basic
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
                df_basic = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df_basic is not None and len(df_basic) > 0:
                    cache_file = os.path.join(CACHE_DIRS['financial'], f"{code}_daily_basic.csv")
                    df_basic.to_csv(cache_file, index=False)
                
                if df_fina is not None and len(df_fina) > 0 and df_basic is not None and len(df_basic) > 0:
                    success += 1
                else:
                    fail += 1
                
                await asyncio.sleep(0.2)
                
            except Exception as e:
                logger.debug(f"{code} 失败: {e}")
                fail += 1
        
        if i + batch_size < len(codes):
            logger.info("等待10秒...")
            await asyncio.sleep(10)
    
    logger.info(f"财务数据同步完成: 成功 {success}, 失败 {fail}")


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='数据补齐脚本')
    parser.add_argument('task', choices=['index', 'day', 'hour', 'financial', 'all'],
                       help='同步任务类型')
    args = parser.parse_args()
    
    os.makedirs('logs', exist_ok=True)
    
    if args.task == 'index':
        await sync_index_data()
    
    elif args.task == 'day':
        codes = list(get_missing_codes('day'))
        logger.info(f"缺失日线数据: {len(codes)} 只")
        await sync_day_data(codes)
    
    elif args.task == 'hour':
        codes = list(get_missing_codes('hour'))
        logger.info(f"缺失小时线数据: {len(codes)} 只")
        await sync_hour_data(codes)
    
    elif args.task == 'financial':
        codes = list(get_missing_codes('financial'))
        logger.info(f"缺失财务数据: {len(codes)} 只")
        await sync_financial_data(codes)
    
    elif args.task == 'all':
        # 按优先级顺序同步
        logger.info("开始同步所有缺失数据...")
        
        # 1. 大盘数据（最快）
        await sync_index_data()
        
        # 2. 日线数据
        codes = list(get_missing_codes('day'))
        if codes:
            logger.info(f"\n缺失日线数据: {len(codes)} 只")
            await sync_day_data(codes)
        
        # 3. 小时线数据
        codes = list(get_missing_codes('hour'))
        if codes:
            logger.info(f"\n缺失小时线数据: {len(codes)} 只")
            await sync_hour_data(codes)
        
        # 4. 财务数据
        codes = list(get_missing_codes('financial'))
        if codes:
            logger.info(f"\n缺失财务数据: {len(codes)} 只")
            await sync_financial_data(codes)
        
        logger.info("\n" + "=" * 60)
        logger.info("所有数据同步完成！")
        logger.info("=" * 60)


if __name__ == '__main__':
    asyncio.run(main())