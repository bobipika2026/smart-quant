#!/usr/bin/env python
"""
数据完整性检查脚本

检查各数据缓存目录，找出缺失的数据文件
"""
import os
import sys
import sqlite3
from datetime import datetime

# 数据缓存目录
CACHE_DIRS = {
    'day': 'data_cache/day',
    'hour': 'data_cache/hour',
    'minute': 'data_cache/minute',
    'financial': 'data_cache/financial',
    'index': 'data_cache/index',
}

# 大盘指数
INDEX_CODES = ['000001', '399001', '399006', '000016', '000300', '000905', '000852']


def get_all_stock_codes(db_path='smart_quant.db'):
    """从数据库获取所有股票代码"""
    if not os.path.exists(db_path):
        print(f"数据库不存在: {db_path}")
        return []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT code, name, exchange FROM stocks WHERE exchange IN ('SH', 'SZ')")
    stocks = cursor.fetchall()
    conn.close()
    
    return [(row[0], row[1], row[2]) for row in stocks]


def check_missing_data():
    """检查缺失的数据"""
    
    print("=" * 70)
    print(f"数据完整性检查报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 获取股票列表
    stocks = get_all_stock_codes()
    print(f"\n数据库股票总数: {len(stocks)}")
    
    if not stocks:
        print("没有股票数据，请先同步股票列表")
        return
    
    stock_codes = set([s[0] for s in stocks])
    
    # 检查各类数据
    missing_report = {}
    
    # 1. 日线数据
    print("\n[1] 检查日线数据...")
    day_dir = CACHE_DIRS['day']
    if os.path.exists(day_dir):
        day_files = set(f.replace('_day.csv', '') for f in os.listdir(day_dir) if f.endswith('_day.csv'))
        missing_day = stock_codes - day_files
        missing_report['day'] = list(missing_day)
        print(f"    已有: {len(day_files)}, 缺失: {len(missing_day)}")
    else:
        missing_report['day'] = list(stock_codes)
        print(f"    目录不存在，缺失: {len(stocks)}")
    
    # 2. 小时线数据
    print("\n[2] 检查小时线数据...")
    hour_dir = CACHE_DIRS['hour']
    if os.path.exists(hour_dir):
        hour_files = set(f.replace('_60min.csv', '') for f in os.listdir(hour_dir) if f.endswith('_60min.csv'))
        missing_hour = stock_codes - hour_files
        missing_report['hour'] = list(missing_hour)
        print(f"    已有: {len(hour_files)}, 缺失: {len(missing_hour)}")
    else:
        missing_report['hour'] = list(stock_codes)
        print(f"    目录不存在，缺失: {len(stocks)}")
    
    # 3. 1分钟数据
    print("\n[3] 检查1分钟数据...")
    min_dir = CACHE_DIRS['minute']
    if os.path.exists(min_dir):
        min_files = set(f.replace('_1min.csv', '') for f in os.listdir(min_dir) if f.endswith('_1min.csv'))
        missing_min = stock_codes - min_files
        missing_report['minute'] = list(missing_min)
        print(f"    已有: {len(min_files)}, 缺失: {len(missing_min)}")
    else:
        missing_report['minute'] = list(stock_codes)
        print(f"    目录不存在，缺失: {len(stocks)}")
    
    # 4. 财务数据 (检查 fina_indicator 和 daily_basic)
    print("\n[4] 检查财务数据...")
    fin_dir = CACHE_DIRS['financial']
    if os.path.exists(fin_dir):
        fina_files = set(f.replace('_fina_indicator.csv', '') for f in os.listdir(fin_dir) if f.endswith('_fina_indicator.csv'))
        basic_files = set(f.replace('_daily_basic.csv', '') for f in os.listdir(fin_dir) if f.endswith('_daily_basic.csv'))
        complete_fin = fina_files & basic_files  # 两种都有才算完整
        missing_fin = stock_codes - complete_fin
        missing_report['financial'] = list(missing_fin)
        print(f"    fina_indicator: {len(fina_files)}, daily_basic: {len(basic_files)}")
        print(f"    完整: {len(complete_fin)}, 缺失: {len(missing_fin)}")
    else:
        missing_report['financial'] = list(stock_codes)
        print(f"    目录不存在，缺失: {len(stocks)}")
    
    # 5. 大盘数据
    print("\n[5] 检查大盘数据...")
    index_dir = CACHE_DIRS['index']
    missing_index = []
    for code in INDEX_CODES:
        index_file = f"{code}_index.csv"
        if not os.path.exists(os.path.join(index_dir, index_file)):
            missing_index.append(code)
    missing_report['index'] = missing_index
    print(f"    已有: {len(INDEX_CODES) - len(missing_index)}, 缺失: {len(missing_index)}")
    if missing_index:
        print(f"    缺失指数: {missing_index}")
    
    # 汇总
    print("\n" + "=" * 70)
    print("缺失汇总:")
    print("=" * 70)
    for data_type, codes in missing_report.items():
        print(f"  {data_type}: {len(codes)}")
    
    # 保存缺失列表到文件
    report_file = "logs/missing_data_report.txt"
    os.makedirs("logs", exist_ok=True)
    with open(report_file, 'w') as f:
        f.write(f"数据缺失报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")
        for data_type, codes in missing_report.items():
            f.write(f"\n[{data_type}] 缺失 {len(codes)} 只:\n")
            # 每行10个
            for i in range(0, min(len(codes), 100), 10):
                f.write(", ".join(codes[i:i+10]) + "\n")
            if len(codes) > 100:
                f.write(f"... 等共 {len(codes)} 只\n")
    
    print(f"\n详细报告已保存到: {report_file}")
    
    return missing_report


if __name__ == '__main__':
    check_missing_data()