"""
策略引擎
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from abc import ABC, abstractmethod


class BaseStrategy(ABC):
    """策略基类"""
    
    def __init__(self, name: str, params: Dict = None):
        self.name = name
        self.params = params or {}
    
    @abstractmethod
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        生成交易信号
        
        Args:
            df: 包含OHLCV数据的DataFrame
        
        Returns:
            DataFrame: 包含信号的数据
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """验证数据是否包含必要字段"""
        required = ['open', 'high', 'low', 'close', 'volume']
        # 转换为小写比较
        cols_lower = [c.lower() for c in df.columns]
        return all(r in cols_lower for r in required)


class MAStrategy(BaseStrategy):
    """均线金叉策略"""
    
    def __init__(self, short_period: int = 5, long_period: int = 20):
        super().__init__(
            name="均线金叉",
            params={"short_period": short_period, "long_period": long_period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        # 确保列名正确
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        # 计算均线
        short_period = self.params.get('short_period', 5)
        long_period = self.params.get('long_period', 20)
        
        df['ma_short'] = df[close_col].rolling(window=short_period).mean()
        df['ma_long'] = df[close_col].rolling(window=long_period).mean()
        
        # 生成信号
        df['signal'] = 0
        df.loc[df['ma_short'] > df['ma_long'], 'signal'] = 1  # 买入
        df.loc[df['ma_short'] < df['ma_long'], 'signal'] = -1  # 卖出
        
        # 检测金叉死叉
        df['golden_cross'] = (
            (df['ma_short'] > df['ma_long']) & 
            (df['ma_short'].shift(1) <= df['ma_long'].shift(1))
        )
        df['death_cross'] = (
            (df['ma_short'] < df['ma_long']) & 
            (df['ma_short'].shift(1) >= df['ma_long'].shift(1))
        )
        
        return df


class MACDStrategy(BaseStrategy):
    """MACD策略"""
    
    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        super().__init__(
            name="MACD策略",
            params={"fast": fast, "slow": slow, "signal": signal}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        fast = self.params.get('fast', 12)
        slow = self.params.get('slow', 26)
        signal_period = self.params.get('signal', 9)
        
        # 计算MACD
        ema_fast = df[close_col].ewm(span=fast).mean()
        ema_slow = df[close_col].ewm(span=slow).mean()
        df['macd'] = ema_fast - ema_slow
        df['macd_signal'] = df['macd'].ewm(span=signal_period).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # 生成信号
        df['signal'] = 0
        df.loc[df['macd'] > df['macd_signal'], 'signal'] = 1
        df.loc[df['macd'] < df['macd_signal'], 'signal'] = -1
        
        return df


class KDJStrategy(BaseStrategy):
    """KDJ策略"""
    
    def __init__(self, n: int = 9, m1: int = 3, m2: int = 3, 
                 oversold: float = 20, overbought: float = 80):
        super().__init__(
            name="KDJ策略",
            params={
                "n": n, "m1": m1, "m2": m2,
                "oversold": oversold, "overbought": overbought
            }
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        n = self.params.get('n', 9)
        m1 = self.params.get('m1', 3)
        m2 = self.params.get('m2', 3)
        oversold = self.params.get('oversold', 20)
        
        # 计算KDJ
        low_n = df[low_col].rolling(window=n).min()
        high_n = df[high_col].rolling(window=n).max()
        
        rsv = (df[close_col] - low_n) / (high_n - low_n) * 100
        df['k'] = rsv.ewm(alpha=1/m1).mean()
        df['d'] = df['k'].ewm(alpha=1/m2).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']
        
        # 生成信号
        df['signal'] = 0
        df.loc[df['j'] < oversold, 'signal'] = 1  # 超卖，买入信号
        df.loc[df['j'] > 100 - oversold, 'signal'] = -1  # 超买，卖出信号
        
        return df


# 策略注册表
STRATEGY_REGISTRY = {
    "ma_cross": MAStrategy,
    "macd": MACDStrategy,
    "kdj": KDJStrategy,
}


def get_strategy(name: str, **params) -> BaseStrategy:
    """获取策略实例"""
    strategy_class = STRATEGY_REGISTRY.get(name)
    if not strategy_class:
        raise ValueError(f"未找到策略: {name}")
    return strategy_class(**params)


def list_strategies() -> List[Dict]:
    """列出所有可用策略"""
    strategies = []
    for name, cls in STRATEGY_REGISTRY.items():
        strategies.append({
            "id": name,
            "name": cls(name="").name,
            "params": cls(name="").params
        })
    return strategies