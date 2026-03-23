"""
组合数据缓存服务
- Tushare日线：5年历史
- AkShare 60分钟：2年历史
- AkShare 1分钟：最近10天
均为前复权数据，带比对校验
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import asyncio
import tushare as ts
import akshare as ak
from app.config import settings


class DataSyncService:
    """数据同步服务"""
    
    # 缓存目录
    CACHE_DIR = "data_cache"
    DAY_DIR = "data_cache/day"       # 日线
    HOUR_DIR = "data_cache/hour"     # 60分钟
    MIN_DIR = "data_cache/minute"    # 1分钟
    
    def __init__(self):
        # 创建缓存目录
        for d in [self.CACHE_DIR, self.DAY_DIR, self.HOUR_DIR, self.MIN_DIR]:
            os.makedirs(d, exist_ok=True)
        
        # 初始化Tushare
        if settings.TUSHARE_TOKEN:
            ts.set_token(settings.TUSHARE_TOKEN)
    
    async def sync_day_data_tushare(self, code: str, years: int = 10) -> pd.DataFrame:
        """
        同步日线数据（Tushare，5年历史）
        
        Args:
            code: 股票代码
            years: 历史年数
        
        Returns:
            DataFrame: 日线数据
        """
        print(f"[日线] 同步 {code} (Tushare, {years}年)")
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=years*365)).strftime('%Y%m%d')
        
        # Tushare代码格式
        ts_code = f"{code}.SZ" if code.startswith(('0', '3')) else f"{code}.SH"
        
        try:
            # 获取前复权数据
            pro = ts.pro_api()
            df = ts.pro_bar(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adj='qfq'  # 前复权
            )
            
            if df is None or len(df) == 0:
                print(f"[日线] {code} 无数据")
                return pd.DataFrame()
            
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
            
            # 按日期排序
            df = df.sort_values('日期').reset_index(drop=True)
            
            # 保存缓存
            cache_file = os.path.join(self.DAY_DIR, f"{code}_day.csv")
            df.to_csv(cache_file, index=False)
            
            print(f"[日线] {code}: {len(df)}条, {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")
            return df
            
        except Exception as e:
            print(f"[日线] {code} 同步失败: {e}")
            return pd.DataFrame()
    
    async def sync_hour_data_akshare(self, code: str) -> pd.DataFrame:
        """
        同步60分钟数据（AkShare，约2年历史）
        
        Args:
            code: 股票代码
        
        Returns:
            DataFrame: 60分钟数据
        """
        print(f"[60分钟] 同步 {code} (AkShare)")
        
        # 股票代码格式
        symbol = f"sh{code}" if code.startswith(('6', '9')) else f"sz{code}"
        
        try:
            # 获取60分钟线（前复权）
            df = ak.stock_zh_a_minute(symbol=symbol, period='60', adjust='qfq')
            
            if df is None or len(df) == 0:
                print(f"[60分钟] {code} 无数据")
                return pd.DataFrame()
            
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
            
            # 保存缓存
            cache_file = os.path.join(self.HOUR_DIR, f"{code}_60min.csv")
            df.to_csv(cache_file, index=False)
            
            # 计算时间跨度
            start = pd.to_datetime(df['时间'].iloc[0])
            end = pd.to_datetime(df['时间'].iloc[-1])
            days = (end - start).days
            
            print(f"[60分钟] {code}: {len(df)}条, 跨度{days}天")
            return df
            
        except Exception as e:
            print(f"[60分钟] {code} 同步失败: {e}")
            return pd.DataFrame()
    
    async def sync_minute_data_akshare(self, code: str) -> pd.DataFrame:
        """
        同步1分钟数据（AkShare，约10天历史）
        
        Args:
            code: 股票代码
        
        Returns:
            DataFrame: 1分钟数据
        """
        print(f"[1分钟] 同步 {code} (AkShare)")
        
        # 股票代码格式
        symbol = f"sh{code}" if code.startswith(('6', '9')) else f"sz{code}"
        
        try:
            # 获取1分钟线（前复权）
            df = ak.stock_zh_a_minute(symbol=symbol, period='1', adjust='qfq')
            
            if df is None or len(df) == 0:
                print(f"[1分钟] {code} 无数据")
                return pd.DataFrame()
            
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
            
            # 保存缓存
            cache_file = os.path.join(self.MIN_DIR, f"{code}_1min.csv")
            df.to_csv(cache_file, index=False)
            
            # 计算时间跨度
            start = pd.to_datetime(df['时间'].iloc[0])
            end = pd.to_datetime(df['时间'].iloc[-1])
            days = (end - start).days
            
            print(f"[1分钟] {code}: {len(df)}条, 跨度{days}天")
            return df
            
        except Exception as e:
            print(f"[1分钟] {code} 同步失败: {e}")
            return pd.DataFrame()
    
    def validate_data(self, code: str) -> Dict:
        """
        校验数据一致性
        
        检查：
        1. 日线收盘价与60分钟收盘价对齐
        2. 60分钟收盘价与1分钟收盘价对齐
        3. 数据完整性
        """
        print(f"\n=== 校验 {code} 数据 ===")
        
        results = {
            'code': code,
            'day': {'count': 0, 'range': None},
            'hour': {'count': 0, 'range': None},
            'minute': {'count': 0, 'range': None},
            'validation': {
                'day_hour_match': None,
                'hour_minute_match': None,
                'errors': []
            }
        }
        
        # 读取日线
        day_file = os.path.join(self.DAY_DIR, f"{code}_day.csv")
        if os.path.exists(day_file):
            df_day = pd.read_csv(day_file)
            results['day']['count'] = len(df_day)
            if len(df_day) > 0:
                results['day']['range'] = f"{df_day['日期'].iloc[0]} ~ {df_day['日期'].iloc[-1]}"
        
        # 读取60分钟
        hour_file = os.path.join(self.HOUR_DIR, f"{code}_60min.csv")
        if os.path.exists(hour_file):
            df_hour = pd.read_csv(hour_file)
            results['hour']['count'] = len(df_hour)
            if len(df_hour) > 0:
                results['hour']['range'] = f"{df_hour['时间'].iloc[0]} ~ {df_hour['时间'].iloc[-1]}"
        
        # 读取1分钟
        min_file = os.path.join(self.MIN_DIR, f"{code}_1min.csv")
        if os.path.exists(min_file):
            df_min = pd.read_csv(min_file)
            results['minute']['count'] = len(df_min)
            if len(df_min) > 0:
                results['minute']['range'] = f"{df_min['时间'].iloc[0]} ~ {df_min['时间'].iloc[-1]}"
        
        # 校验日线与60分钟对齐
        if os.path.exists(day_file) and os.path.exists(hour_file):
            df_day = pd.read_csv(day_file)
            df_hour = pd.read_csv(hour_file)
            
            if len(df_day) > 0 and len(df_hour) > 0:
                # 获取最近一个交易日的数据
                last_day = df_day['日期'].iloc[-1]
                last_day_close = df_day['收盘'].iloc[-1]
                
                # 60分钟数据中同一天的收盘价
                df_hour['日期'] = pd.to_datetime(df_hour['时间']).dt.strftime('%Y-%m-%d')
                hour_last_day = df_hour[df_hour['日期'] == last_day]
                
                if len(hour_last_day) > 0:
                    hour_last_close = hour_last_day['收盘'].iloc[-1]
                    diff = abs(last_day_close - hour_last_close) / last_day_close * 100
                    
                    if diff < 0.1:  # 差异小于0.1%
                        results['validation']['day_hour_match'] = True
                        print(f"  日线-60分钟对齐: ✅ 差异 {diff:.4f}%")
                    else:
                        results['validation']['day_hour_match'] = False
                        results['validation']['errors'].append(f"日线-60分钟差异 {diff:.2f}%")
                        print(f"  日线-60分钟对齐: ❌ 差异 {diff:.4f}%")
        
        # 校验60分钟与1分钟对齐
        if os.path.exists(hour_file) and os.path.exists(min_file):
            df_hour = pd.read_csv(hour_file)
            df_min = pd.read_csv(min_file)
            
            if len(df_hour) > 0 and len(df_min) > 0:
                # 获取最近一个交易小时的收盘价
                last_hour_time = df_hour['时间'].iloc[-1]
                last_hour_close = df_hour['收盘'].iloc[-1]
                
                # 1分钟数据中同一时间的收盘价
                min_last = df_min[df_min['时间'].str[:16] == last_hour_time[:16]]
                
                if len(min_last) > 0:
                    min_last_close = min_last['收盘'].iloc[-1]
                    diff = abs(last_hour_close - min_last_close) / last_hour_close * 100
                    
                    if diff < 0.1:
                        results['validation']['hour_minute_match'] = True
                        print(f"  60分钟-1分钟对齐: ✅ 差异 {diff:.4f}%")
                    else:
                        results['validation']['hour_minute_match'] = False
                        results['validation']['errors'].append(f"60分钟-1分钟差异 {diff:.2f}%")
                        print(f"  60分钟-1分钟对齐: ❌ 差异 {diff:.4f}%")
        
        return results
    
    async def sync_all(self, codes: List[str]) -> Dict:
        """
        同步所有数据源
        
        Args:
            codes: 股票代码列表
        
        Returns:
            同步结果
        """
        print("=" * 60)
        print("组合数据同步")
        print("  - Tushare日线: 5年历史")
        print("  - AkShare 60分钟: 2年历史")
        print("  - AkShare 1分钟: 10天历史")
        print("=" * 60)
        
        results = {}
        
        for code in codes:
            print(f"\n>>> 同步 {code}")
            results[code] = {}
            
            # 1. 日线（Tushare）
            df_day = await self.sync_day_data_tushare(code, years=5)
            results[code]['day'] = len(df_day)
            
            # 等待一下避免Tushare限频
            await asyncio.sleep(1)
            
            # 2. 60分钟（AkShare）
            df_hour = await self.sync_hour_data_akshare(code)
            results[code]['hour'] = len(df_hour)
            
            # 3. 1分钟（AkShare）
            df_min = await self.sync_minute_data_akshare(code)
            results[code]['minute'] = len(df_min)
            
            # 4. 校验
            validation = self.validate_data(code)
            results[code]['validation'] = validation['validation']
        
        # 汇总
        print("\n" + "=" * 60)
        print("同步完成汇总")
        print("=" * 60)
        print(f"{'代码':<10} {'日线':<10} {'60分钟':<10} {'1分钟':<10} {'校验'}")
        print("-" * 60)
        for code, r in results.items():
            v_status = "✅" if r['validation'].get('day_hour_match') and r['validation'].get('hour_minute_match') else "⚠️"
            print(f"{code:<10} {r['day']:<10} {r['hour']:<10} {r['minute']:<10} {v_status}")
        
        return results


# 命令行入口
async def main():
    import sys
    
    # 默认同步银行股
    codes = ["000001", "002142", "600000", "600036", "601166", "601398", "601939"]
    
    if len(sys.argv) > 1:
        codes = sys.argv[1:]
    
    service = DataSyncService()
    await service.sync_all(codes)


if __name__ == '__main__':
    asyncio.run(main())