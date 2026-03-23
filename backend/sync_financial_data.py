#!/usr/bin/env python3
"""
同步股票财务数据（10年）
使用Tushare Pro API

数据类型：
1. daily_basic - 每日基本面指标（PE、PB、市值等）
2. fina_indicator - 财务指标（ROE、毛利率、净利率等）
3. balancesheet - 资产负债表
4. income - 利润表
5. cashflow - 现金流量表
"""
import os
import sys
import asyncio
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta
import functools
from typing import List, Optional
import time

print = functools.partial(print, flush=True)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 从环境变量获取Tushare Token
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 尝试从.env文件直接读取
if not os.getenv('TUSHARE_TOKEN'):
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if line.startswith('TUSHARE_TOKEN='):
                    os.environ['TUSHARE_TOKEN'] = line.strip().split('=', 1)[1]
                    break

TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')

if not TUSHARE_TOKEN:
    print("❌ 未找到TUSHARE_TOKEN，请检查.env文件")
    sys.exit(1)

# 初始化Tushare
pro = ts.pro_api(TUSHARE_TOKEN)

# 财务数据目录
DATA_DIR = "data_cache/financial"
os.makedirs(DATA_DIR, exist_ok=True)


def get_filtered_stocks() -> List[str]:
    """获取股票列表"""
    stocks_file = 'filtered_stocks.txt'
    if os.path.exists(stocks_file):
        with open(stocks_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    
    # 从数据库获取
    import sqlite3
    conn = sqlite3.connect('smart_quant.db')
    cursor = conn.cursor()
    cursor.execute("SELECT code FROM stocks ORDER BY code")
    stocks = [row[0] for row in cursor.fetchall()]
    conn.close()
    return stocks


def ts_code(code: str) -> str:
    """转换为Tushare代码格式"""
    if code.startswith(('6', '9')):
        return f"{code}.SH"
    else:
        return f"{code}.SZ"


def fetch_with_retry(func, max_retries=3, delay=1):
    """带重试的获取函数"""
    for i in range(max_retries):
        try:
            result = func()
            return result
        except Exception as e:
            if i < max_retries - 1:
                print(f"重试 {i+1}/{max_retries}: {e}")
                time.sleep(delay * (i + 1))
            else:
                raise e


def sync_daily_basic(code: str, years: int = 10) -> pd.DataFrame:
    """
    同步每日基本面指标
    包括：PE、PB、PS、市值、换手率等
    """
    ts_cd = ts_code(code)
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = pro.daily_basic(
            ts_code=ts_cd,
            start_date=start_date,
            end_date=end_date,
            fields='ts_code,trade_date,close,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv,turnover_rate,volume_ratio,pe,pb'
        )
        return df
    except Exception as e:
        print(f"  daily_basic失败: {e}")
        return pd.DataFrame()


def sync_fina_indicator(code: str, years: int = 10) -> pd.DataFrame:
    """
    同步财务指标
    包括：ROE、ROA、毛利率、净利率等
    """
    ts_cd = ts_code(code)
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = pro.fina_indicator(
            ts_code=ts_cd,
            start_date=start_date,
            end_date=end_date
        )
        return df
    except Exception as e:
        print(f"  fina_indicator失败: {e}")
        return pd.DataFrame()


def sync_income(code: str, years: int = 10) -> pd.DataFrame:
    """
    同步利润表
    包括：营收、利润、费用等
    """
    ts_cd = ts_code(code)
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = pro.income(
            ts_code=ts_cd,
            start_date=start_date,
            end_date=end_date
        )
        return df
    except Exception as e:
        print(f"  income失败: {e}")
        return pd.DataFrame()


def sync_balance_sheet(code: str, years: int = 10) -> pd.DataFrame:
    """
    同步资产负债表
    """
    ts_cd = ts_code(code)
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = pro.balancesheet(
            ts_code=ts_cd,
            start_date=start_date,
            end_date=end_date
        )
        return df
    except Exception as e:
        print(f"  balancesheet失败: {e}")
        return pd.DataFrame()


def sync_cashflow(code: str, years: int = 10) -> pd.DataFrame:
    """
    同步现金流量表
    """
    ts_cd = ts_code(code)
    
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
    
    try:
        df = pro.cashflow(
            ts_code=ts_cd,
            start_date=start_date,
            end_date=end_date
        )
        return df
    except Exception as e:
        print(f"  cashflow失败: {e}")
        return pd.DataFrame()


def save_financial_data(code: str, data_type: str, df: pd.DataFrame):
    """保存财务数据"""
    if df.empty:
        return
    
    file_path = os.path.join(DATA_DIR, f"{code}_{data_type}.csv")
    df.to_csv(file_path, index=False)
    print(f"  {data_type}: {len(df)}条")


def sync_stock_financial(code: str, years: int = 10):
    """同步单只股票的所有财务数据"""
    
    # 1. 每日基本面指标
    df_basic = sync_daily_basic(code, years)
    if not df_basic.empty:
        save_financial_data(code, 'daily_basic', df_basic)
    
    time.sleep(0.3)  # 避免限频
    
    # 2. 财务指标
    df_fina = sync_fina_indicator(code, years)
    if not df_fina.empty:
        save_financial_data(code, 'fina_indicator', df_fina)
    
    time.sleep(0.3)
    
    # 3. 利润表
    df_income = sync_income(code, years)
    if not df_income.empty:
        save_financial_data(code, 'income', df_income)
    
    time.sleep(0.3)
    
    # 4. 资产负债表
    df_balance = sync_balance_sheet(code, years)
    if not df_balance.empty:
        save_financial_data(code, 'balance', df_balance)
    
    time.sleep(0.3)
    
    # 5. 现金流量表
    df_cashflow = sync_cashflow(code, years)
    if not df_cashflow.empty:
        save_financial_data(code, 'cashflow', df_cashflow)


def main():
    start_time = datetime.now()
    
    print("=" * 60)
    print("同步股票财务数据（10年）")
    print("=" * 60)
    print(f"开始时间: {start_time}")
    
    # 获取股票列表
    stocks = get_filtered_stocks()
    print(f"\n股票总数: {len(stocks)}")
    
    success, failed = 0, 0
    
    for i, code in enumerate(stocks):
        print(f"\n[{i+1}/{len(stocks)}] {code}")
        
        try:
            sync_stock_financial(code, years=10)
            success += 1
        except Exception as e:
            failed += 1
            print(f"  ❌ 失败: {e}")
        
        # 避免API限频
        time.sleep(0.5)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print(f"同步完成!")
    print(f"成功: {success}, 失败: {failed}")
    print(f"耗时: {duration/60:.1f} 分钟")
    print(f"结束时间: {datetime.now()}")
    print("=" * 60)


if __name__ == '__main__':
    main()