#!/usr/bin/env python3
"""
筛选符合条件的股票：
1. 剔除ST股票
2. 剔除市值低于50亿的股票
"""
import sys
import os
import sqlite3
import tushare as ts
import pandas as pd
from datetime import datetime

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 从环境变量获取token
from dotenv import load_dotenv
load_dotenv()

token = os.getenv('TUSHARE_TOKEN')
if not token:
    print("❌ 未找到TUSHARE_TOKEN，请检查.env文件")
    sys.exit(1)

ts.set_token(token)
pro = ts.pro_api()

def get_stock_list():
    """获取数据库中的股票列表"""
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT code, name FROM stocks")
    stocks = cursor.fetchall()
    conn.close()
    return stocks

def filter_st_stocks(stocks):
    """剔除ST股票"""
    filtered = [(code, name) for code, name in stocks 
                if 'ST' not in name and '*ST' not in name]
    return filtered

def get_market_cap(codes):
    """
    获取股票市值
    使用Tushare daily_basic接口
    """
    print(f"获取 {len(codes)} 只股票的市值数据...")
    
    # 按批次获取（每次1000只）
    batch_size = 1000
    all_data = []
    
    for i in range(0, len(codes), batch_size):
        batch = codes[i:i+batch_size]
        ts_codes = [f"{c}.SZ" if c.startswith(('0', '3')) else f"{c}.SH" for c in batch]
        
        try:
            # 获取最近一个交易日的市值数据
            df = pro.daily_basic(
                ts_code=','.join(ts_codes),
                fields='ts_code,total_mv,circ_mv'
            )
            all_data.append(df)
            print(f"  批次 {i//batch_size + 1}: {len(df)} 只")
        except Exception as e:
            print(f"  批次失败: {e}")
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        return result
    return pd.DataFrame()

def main():
    print("=" * 60)
    print("股票筛选")
    print("=" * 60)
    
    # 1. 获取所有股票
    stocks = get_stock_list()
    print(f"\n数据库总股票数: {len(stocks)}")
    
    # 2. 剔除ST
    non_st = filter_st_stocks(stocks)
    print(f"剔除ST后: {len(non_st)}")
    
    # 3. 获取市值
    codes = [code for code, name in non_st]
    market_cap_df = get_market_cap(codes)
    
    if market_cap_df.empty:
        print("\n❌ 无法获取市值数据，请检查Tushare token")
        return
    
    # 4. 筛选市值>=50亿
    market_cap_df['code'] = market_cap_df['ts_code'].str[:6]
    market_cap_df['total_mv_yi'] = market_cap_df['total_mv']  # 总市值（亿元）
    
    # 筛选市值>=50亿
    filtered = market_cap_df[market_cap_df['total_mv_yi'] >= 50]
    
    print(f"\n市值>=50亿的股票: {len(filtered)}")
    
    # 5. 保存结果
    result_codes = filtered['code'].tolist()
    
    with open('filtered_stocks.txt', 'w') as f:
        for code in result_codes:
            f.write(code + '\n')
    
    print(f"\n已保存到 filtered_stocks.txt")
    
    # 6. 显示市值分布
    print("\n市值分布:")
    print(f"  50-100亿: {len(filtered[(filtered['total_mv_yi'] >= 50) & (filtered['total_mv_yi'] < 100)])}")
    print(f"  100-500亿: {len(filtered[(filtered['total_mv_yi'] >= 100) & (filtered['total_mv_yi'] < 500)])}")
    print(f"  500-1000亿: {len(filtered[(filtered['total_mv_yi'] >= 500) & (filtered['total_mv_yi'] < 1000)])}")
    print(f"  1000亿以上: {len(filtered[filtered['total_mv_yi'] >= 1000])}")
    
    return result_codes

if __name__ == '__main__':
    main()