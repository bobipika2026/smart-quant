"""
数据服务
"""
import akshare as ak
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta


class DataService:
    """数据获取服务"""
    
    @staticmethod
    async def get_realtime_quotes(codes: List[str] = None) -> pd.DataFrame:
        """
        获取实时行情数据
        
        Args:
            codes: 股票代码列表，为空则获取全部A股
        
        Returns:
            DataFrame: 行情数据
        """
        try:
            df = ak.stock_zh_a_spot_em()
            if codes:
                df = df[df['代码'].isin(codes)]
            return df
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return pd.DataFrame()
    
    @staticmethod
    async def get_stock_history(
        code: str,
        start_date: str = None,
        end_date: str = None,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """
        获取股票历史数据
        
        Args:
            code: 股票代码
            start_date: 开始日期 YYYYMMDD
            end_date: 结束日期 YYYYMMDD
            adjust: 复权类型 qfq-前复权 hfq-后复权 不填-不复权
        
        Returns:
            DataFrame: 历史数据
        """
        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_date or "20200101",
                end_date=end_date or datetime.now().strftime("%Y%m%d"),
                adjust=adjust
            )
            return df
        except Exception as e:
            print(f"获取历史数据失败: {e}")
            return pd.DataFrame()
    
    @staticmethod
    async def get_stock_info(code: str) -> dict:
        """
        获取股票基本信息
        
        Args:
            code: 股票代码
        
        Returns:
            dict: 股票信息
        """
        try:
            df = ak.stock_individual_info_em(symbol=code)
            info = dict(zip(df['item'], df['value']))
            return info
        except Exception as e:
            print(f"获取股票信息失败: {e}")
            return {}
    
    @staticmethod
    async def search_stocks(keyword: str) -> pd.DataFrame:
        """
        搜索股票
        
        Args:
            keyword: 搜索关键词（代码或名称）
        
        Returns:
            DataFrame: 匹配的股票列表
        """
        try:
            df = ak.stock_zh_a_spot_em()
            # 搜索代码或名称
            mask = (
                df['代码'].str.contains(keyword, na=False) |
                df['名称'].str.contains(keyword, na=False)
            )
            return df[mask].head(20)
        except Exception as e:
            print(f"搜索股票失败: {e}")
            return pd.DataFrame()