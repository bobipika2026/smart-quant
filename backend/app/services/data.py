"""
数据服务 - 使用新浪财经API
支持实时行情、历史数据、股票搜索
"""
import httpx
import pandas as pd
import numpy as np
import re
from typing import List, Optional
from datetime import datetime, timedelta
import asyncio


class DataService:
    """数据获取服务 - 新浪财经数据源"""
    
    # 新浪财经API基础URL
    SINA_QUOTE_URL = "https://hq.sinajs.cn/list={}"
    SINA_REALTIME_URL = "https://hq.sinajs.cn/list={}"
    
    # 行业分类
    INDUSTRY_MAP = {
        "银行": ["000001", "600000", "600036", "601398", "601939"],
        "白酒": ["000858", "600519"],
        "家电": ["000333", "000651"],
        "汽车": ["002594"],
        "科技": ["000063", "002415", "300750"],
    }
    
    @staticmethod
    async def _fetch_sina_data(codes: List[str]) -> dict:
        """从新浪财经获取股票数据"""
        if not codes:
            return {}
        
        codes_str = ",".join([f"sh{c}" if c.startswith("6") else f"sz{c}" for c in codes])
        url = f"https://hq.sinajs.cn/list={codes_str}"
        
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, headers={
                    'Referer': 'https://finance.sina.com.cn/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
            if resp.status_code != 200:
                return {}
            
            result = {}
            lines = resp.text.strip().split('\n')
            for line in lines:
                match = re.match(r'var hq_str_(sh|sz)(\d+)="(.*)";', line)
                if match:
                    market, code, data = match.groups()
                    if data:  # 确保有数据
                        fields = data.split(',')
                        if len(fields) >= 32:  # 完整数据有32个字段
                            result[code] = {
                                '名称': fields[0],
                                '开盘': float(fields[1]) if fields[1] else 0,
                                '昨收': float(fields[2]) if fields[2] else 0,
                                '最新价': float(fields[3]) if fields[3] else 0,
                                '最高': float(fields[4]) if fields[4] else 0,
                                '最低': float(fields[5]) if fields[5] else 0,
                                '成交量': int(float(fields[8])) if fields[8] else 0,
                                '成交额': float(fields[9]) if fields[9] else 0,
                            }
                            # 计算涨跌幅
                            if result[code]['昨收'] > 0:
                                result[code]['涨跌幅'] = round(
                                    (result[code]['最新价'] - result[code]['昨收']) / result[code]['昨收'] * 100, 2
                                )
                            else:
                                result[code]['涨跌幅'] = 0
            return result
        except Exception as e:
            print(f"[新浪API] 获取数据失败: {e}")
            return {}
    
    @staticmethod
    async def get_realtime_quotes(codes: List[str] = None) -> pd.DataFrame:
        """获取实时行情数据"""
        # 如果没有指定代码，获取热门股票
        if not codes:
            codes = [
                "000001", "000002", "000063", "000333", "000651", "000858",
                "002415", "002594", "300750",
                "600000", "600036", "600519", "601318", "601398", "601857"
            ]
        
        data = await DataService._fetch_sina_data(codes)
        
        if not data:
            return pd.DataFrame()
        
        df = pd.DataFrame([
            {"代码": code, **info}
            for code, info in data.items()
        ])
        return df
    
    @staticmethod
    async def get_stock_history(
        code: str,
        start_date: str = None,
        end_date: str = None,
        adjust: str = "qfq"
    ) -> pd.DataFrame:
        """获取股票历史数据"""
        # 新浪财经历史数据URL
        market = "sh" if code.startswith("6") else "sz"
        
        # 计算日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        try:
            # 尝试从新浪获取历史数据
            url = f"https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData"
            params = {
                "symbol": f"{market}{code}",
                "scale": "240",
                "ma": "no",
                "datalen": "365"
            }
            
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params)
                
            if resp.status_code == 200:
                import json
                data = resp.json()
                if data and isinstance(data, list):
                    df = pd.DataFrame(data)
                    if len(df) > 0:
                        df = df.rename(columns={
                            'day': '日期',
                            'open': '开盘',
                            'close': '收盘',
                            'high': '最高',
                            'low': '最低',
                            'volume': '成交量'
                        })
                        # 转换数据类型
                        for col in ['开盘', '收盘', '最高', '最低', '成交量']:
                            if col in df.columns:
                                df[col] = pd.to_numeric(df[col], errors='coerce')
                        return df
        except Exception as e:
            print(f"[新浪API] 获取历史数据失败: {e}")
        
        # 如果API失败，生成模拟数据
        print(f"[数据] 使用模拟历史数据: {code}")
        np.random.seed(hash(code) % 2**32)
        dates = pd.date_range(start=start_date, periods=100, freq="D")
        base_price = np.random.uniform(10, 100)
        prices = base_price + np.cumsum(np.random.randn(100) * 0.02 * base_price)
        prices = np.maximum(prices, 1)
        
        return pd.DataFrame({
            "日期": dates.strftime("%Y-%m-%d"),
            "开盘": np.round(prices * 0.99, 2),
            "收盘": np.round(prices, 2),
            "最高": np.round(prices * 1.02, 2),
            "最低": np.round(prices * 0.98, 2),
            "成交量": np.random.randint(100000, 1000000, 100)
        })
    
    @staticmethod
    async def get_stock_info(code: str) -> dict:
        """获取股票基本信息"""
        data = await DataService._fetch_sina_data([code])
        if code in data:
            return {"代码": code, **data[code]}
        return {"代码": code, "名称": f"股票{code}"}
    
    @staticmethod
    async def search_stocks(keyword: str) -> pd.DataFrame:
        """搜索股票"""
        keyword = keyword.strip().upper()
        
        # 常用股票列表（用于搜索匹配）
        common_stocks = [
            "000001", "000002", "000063", "000333", "000651", "000858",
            "002415", "002594", "300750",
            "600000", "600036", "600519", "601318", "601398", "601857",
            "601166", "600276", "002304", "300059", "600030"
        ]
        
        # 获取实时数据
        data = await DataService._fetch_sina_data(common_stocks)
        
        if not data:
            return pd.DataFrame()
        
        # 过滤匹配的股票
        results = []
        for code, info in data.items():
            name = info.get('名称', '')
            if keyword in code or keyword in name.upper() or keyword.lower() in name:
                results.append({"代码": code, **info})
        
        return pd.DataFrame(results)