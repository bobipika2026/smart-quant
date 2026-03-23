"""
通达信本地数据解析服务
支持解析 .day (日线) 和 .lc1 (1分钟线) 文件
"""
import struct
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List


class TdxDataService:
    """通达信数据解析服务"""
    
    # 通达信数据目录（常见位置）
    TDX_PATHS = [
        os.path.expanduser("~/Library/Application Support/通达信金融终端/vipdoc"),  # macOS
        os.path.expanduser("~/.tdx/vipdoc"),  # macOS 备选
        os.path.expanduser("~/tdx/vipdoc"),  # 用户目录
        "C:/new_tdx/vipdoc",  # Windows
        "C:/tdx/vipdoc",
    ]
    
    @staticmethod
    def find_tdx_path() -> Optional[str]:
        """查找通达信数据目录"""
        for path in TdxDataService.TDX_PATHS:
            if os.path.exists(path):
                return path
        return None
    
    @staticmethod
    def get_market_code(code: str) -> tuple:
        """
        获取市场代码和通达信代码格式
        
        Args:
            code: 股票代码 (如 000001)
        
        Returns:
            (market, ts_code) 如 ('sz', 'sz000001')
        """
        if code.startswith('6'):
            return 'sh', f'sh{code}'
        elif code.startswith(('0', '3')):
            return 'sz', f'sz{code}'
        else:
            return 'sz', f'sz{code}'
    
    @staticmethod
    def parse_day_file(file_path: str) -> pd.DataFrame:
        """
        解析通达信日线文件 (.day)
        
        文件结构: 每条记录32字节
        - 日期: 4字节 (整数, YYYYMMDD)
        - 开盘价: 4字节 (整数, 实际价格*100)
        - 最高价: 4字节
        - 最低价: 4字节
        - 收盘价: 4字节
        - 成交额: 4字节 (float)
        - 成交量: 4字节 (整数)
        - 保留: 4字节
        """
        if not os.path.exists(file_path):
            return pd.DataFrame()
        
        data = []
        with open(file_path, 'rb') as f:
            while True:
                record = f.read(32)
                if len(record) < 32:
                    break
                
                try:
                    date_int, open_p, high, low, close, amount, volume, _ = \
                        struct.unpack('IIIIIfII', record)
                    
                    # 转换日期
                    year = date_int // 10000
                    month = (date_int // 100) % 100
                    day = date_int % 100
                    
                    if year < 1990 or year > 2100:
                        continue
                    
                    date_str = f"{year:04d}-{month:02d}-{day:02d}"
                    
                    data.append({
                        '日期': date_str,
                        '开盘': open_p / 100.0,
                        '最高': high / 100.0,
                        '最低': low / 100.0,
                        '收盘': close / 100.0,
                        '成交量': volume,
                        '成交额': amount
                    })
                except:
                    continue
        
        df = pd.DataFrame(data)
        if len(df) > 0:
            df = df.sort_values('日期').reset_index(drop=True)
        
        return df
    
    @staticmethod
    def parse_lc1_file(file_path: str) -> pd.DataFrame:
        """
        解析通达信1分钟线文件 (.lc1)
        
        文件结构: 每条记录32字节
        - 日期时间: 4字节 (整数)
        - 开盘价: 4字节 (float)
        - 最高价: 4字节 (float)
        - 最低价: 4字节 (float)
        - 收盘价: 4字节 (float)
        - 成交额: 4字节 (float)
        - 成交量: 4字节 (int)
        - 保留: 4字节
        """
        if not os.path.exists(file_path):
            return pd.DataFrame()
        
        data = []
        current_date = None
        
        with open(file_path, 'rb') as f:
            while True:
                record = f.read(32)
                if len(record) < 32:
                    break
                
                try:
                    # 解析记录
                    date_time, open_p, high, low, close, amount, volume, _ = \
                        struct.unpack('iffffiI', record)
                    
                    # 解析时间
                    hour = date_time // 60
                    minute = date_time % 60
                    
                    # 推断日期（通达信分钟线不存储日期，需要推断）
                    if hour == 9 and minute == 30:
                        # 新交易日开始
                        if current_date:
                            current_date += timedelta(days=1)
                        else:
                            current_date = datetime.now().date()
                    
                    if current_date is None:
                        current_date = datetime.now().date()
                    
                    # 跳过无效数据
                    if hour < 9 or hour > 15:
                        continue
                    if open_p <= 0 or close <= 0:
                        continue
                    
                    time_str = f"{current_date} {hour:02d}:{minute:02d}:00"
                    
                    data.append({
                        '时间': time_str,
                        '开盘': open_p,
                        '最高': high,
                        '最低': low,
                        '收盘': close,
                        '成交量': volume,
                        '成交额': amount
                    })
                except:
                    continue
        
        df = pd.DataFrame(data)
        if len(df) > 0:
            df = df.sort_values('时间').reset_index(drop=True)
        
        return df
    
    @staticmethod
    def get_day_data(code: str, tdx_path: str = None) -> pd.DataFrame:
        """
        获取日线数据
        
        Args:
            code: 股票代码
            tdx_path: 通达信数据目录
        
        Returns:
            DataFrame: 日线数据
        """
        if tdx_path is None:
            tdx_path = TdxDataService.find_tdx_path()
        
        if tdx_path is None:
            print("[通达信] 未找到通达信数据目录")
            return pd.DataFrame()
        
        market, ts_code = TdxDataService.get_market_code(code)
        file_path = os.path.join(tdx_path, market, 'lday', f'{ts_code}.day')
        
        print(f"[通达信] 读取日线: {file_path}")
        return TdxDataService.parse_day_file(file_path)
    
    @staticmethod
    def get_minute_data(code: str, tdx_path: str = None) -> pd.DataFrame:
        """
        获取1分钟线数据
        
        Args:
            code: 股票代码
            tdx_path: 通达信数据目录
        
        Returns:
            DataFrame: 分钟线数据
        """
        if tdx_path is None:
            tdx_path = TdxDataService.find_tdx_path()
        
        if tdx_path is None:
            print("[通达信] 未找到通达信数据目录")
            return pd.DataFrame()
        
        market, ts_code = TdxDataService.get_market_code(code)
        file_path = os.path.join(tdx_path, market, 'minline', f'{ts_code}.lc1')
        
        print(f"[通达信] 读取分钟线: {file_path}")
        return TdxDataService.parse_lc1_file(file_path)
    
    @staticmethod
    def export_to_csv(code: str, output_dir: str = 'data_cache/tdx', tdx_path: str = None):
        """
        导出数据到CSV
        
        Args:
            code: 股票代码
            output_dir: 输出目录
            tdx_path: 通达信数据目录
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # 导出日线
        df_day = TdxDataService.get_day_data(code, tdx_path)
        if len(df_day) > 0:
            day_file = os.path.join(output_dir, f'{code}_day.csv')
            df_day.to_csv(day_file, index=False)
            print(f"[通达信] 导出日线: {day_file} ({len(df_day)}条)")
        
        # 导出分钟线
        df_min = TdxDataService.get_minute_data(code, tdx_path)
        if len(df_min) > 0:
            min_file = os.path.join(output_dir, f'{code}_1min.csv')
            df_min.to_csv(min_file, index=False)
            print(f"[通达信] 导出分钟线: {min_file} ({len(df_min)}条)")
        
        return {
            'day': len(df_day),
            'minute': len(df_min)
        }


# 测试代码
if __name__ == '__main__':
    print("=== 通达信数据解析测试 ===\n")
    
    # 查找通达信目录
    tdx_path = TdxDataService.find_tdx_path()
    if tdx_path:
        print(f"找到通达信目录: {tdx_path}")
        
        # 测试解析
        df_day = TdxDataService.get_day_data('000001', tdx_path)
        print(f"\n日线数据: {len(df_day)}条")
        if len(df_day) > 0:
            print(df_day.head())
        
        df_min = TdxDataService.get_minute_data('000001', tdx_path)
        print(f"\n分钟线数据: {len(df_min)}条")
        if len(df_min) > 0:
            print(df_min.head())
    else:
        print("未找到通达信数据目录")
        print("\n请确保通达信客户端已安装，或将数据文件放到以下位置之一:")
        for path in TdxDataService.TDX_PATHS:
            print(f"  {path}")