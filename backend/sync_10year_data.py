"""
补充10年期历史数据

使用Tushare API获取2016年至今的日线数据
"""
import os
import sys
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import time

# 设置Token
TUSHARE_TOKEN = "21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45"
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DAY_CACHE_DIR = "data_cache/day"

def get_trade_calendar(start_date: str, end_date: str) -> list:
    """获取交易日历"""
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date, is_open='1')
    return sorted(df['cal_date'].tolist())

def get_stock_list() -> list:
    """获取股票列表（低估值板块成分股）"""
    # 主要获取银行、建筑装饰、非银金融、交通运输、煤炭等板块的股票
    # 先获取全部A股
    df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry')
    return df['ts_code'].tolist()

def sync_stock_day_data(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """同步单只股票日线数据"""
    try:
        df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and len(df) > 0:
            df = df.sort_values('trade_date').reset_index(drop=True)
            return df
    except Exception as e:
        print(f"  获取 {ts_code} 失败: {e}")
    return pd.DataFrame()

def merge_with_existing(ts_code: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """合并新数据与现有数据"""
    code = ts_code.split('.')[0]
    file_path = os.path.join(DAY_CACHE_DIR, f"{code}_day.csv")
    
    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path, encoding='utf-8')
        
        # 合并
        if '日期' in existing_df.columns:
            existing_df['trade_date'] = existing_df['日期'].astype(str)
        
        if 'trade_date' in existing_df.columns and 'trade_date' in new_df.columns:
            # 去重合并
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=['trade_date'], keep='last')
            combined = combined.sort_values('trade_date').reset_index(drop=True)
            return combined
    
    return new_df

def save_day_data(ts_code: str, df: pd.DataFrame):
    """保存日线数据"""
    code = ts_code.split('.')[0]
    file_path = os.path.join(DAY_CACHE_DIR, f"{code}_day.csv")
    
    # 转换格式
    save_df = df.copy()
    if 'trade_date' in save_df.columns:
        save_df['日期'] = save_df['trade_date']
    if 'open' in save_df.columns:
        save_df['开盘'] = save_df['open']
    if 'high' in save_df.columns:
        save_df['最高'] = save_df['high']
    if 'low' in save_df.columns:
        save_df['最低'] = save_df['low']
    if 'close' in save_df.columns:
        save_df['收盘'] = save_df['close']
    if 'vol' in save_df.columns:
        save_df['成交量'] = save_df['vol']
    if 'amount' in save_df.columns:
        save_df['成交额'] = save_df['amount']
    
    # 保留需要的列
    cols_to_save = ['ts_code', '日期', '开盘', '最高', '最低', '收盘', 'pre_close', 'change', '涨跌幅', '成交量', '成交额']
    cols_available = [c for c in cols_to_save if c in save_df.columns]
    
    if '涨跌幅' not in save_df.columns and 'pct_chg' in save_df.columns:
        save_df['涨跌幅'] = save_df['pct_chg']
        cols_available.append('涨跌幅')
    
    save_df[cols_available].to_csv(file_path, index=False, encoding='utf-8')
    print(f"  保存 {code}: {len(df)}条数据")

def main():
    print("=" * 60)
    print("补充10年期历史数据")
    print("=" * 60)
    
    # 时间范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = '20160101'  # 2016年开始
    
    print(f"\n数据范围: {start_date} ~ {end_date}")
    
    # 获取低估值板块的股票
    print("\n获取股票列表...")
    stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,industry')
    
    # 筛选目标板块
    target_industries = ['银行', '建筑装饰', '证券', '运输设备', '煤炭开采', '保险', '多元金融']
    
    # 也包含一些大盘股
    stocks_to_sync = []
    
    # 1. 目标板块股票
    for ind in target_industries:
        ind_stocks = stock_basic[stock_basic['industry'].str.contains(ind, na=False)]
        stocks_to_sync.extend(ind_stocks['ts_code'].tolist())
    
    # 2. 沪深300成分股（作为补充）
    try:
        hs300 = pro.index_weight(index_code='399300.SZ', start_date='20200101')
        stocks_to_sync.extend(hs300['con_code'].unique().tolist()[:100])
    except:
        pass
    
    # 去重
    stocks_to_sync = list(set(stocks_to_sync))
    print(f"需要同步 {len(stocks_to_sync)} 只股票")
    
    # 同步数据
    success_count = 0
    for i, ts_code in enumerate(stocks_to_sync[:200]):  # 限制200只
        print(f"\n[{i+1}/{min(len(stocks_to_sync), 200)}] {ts_code}")
        
        # 获取数据
        df = sync_stock_day_data(ts_code, start_date, end_date)
        
        if len(df) > 0:
            # 合并现有数据
            merged = merge_with_existing(ts_code, df)
            
            # 保存
            save_day_data(ts_code, merged)
            success_count += 1
        else:
            print(f"  无数据")
        
        # 避免API限频
        time.sleep(0.3)
    
    print(f"\n{'='*60}")
    print(f"同步完成: {success_count}/{min(len(stocks_to_sync), 200)}只股票")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()