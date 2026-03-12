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


class RSIStrategy(BaseStrategy):
    """RSI相对强弱指标策略"""
    
    def __init__(self, period: int = 14, oversold: float = 30, overbought: float = 70):
        super().__init__(
            name="RSI策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', 30)
        overbought = self.params.get('overbought', 70)
        
        # 计算RSI
        delta = df[close_col].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 生成信号
        df['signal'] = 0
        # RSI低于超卖线，买入信号
        df.loc[df['rsi'] < oversold, 'signal'] = 1
        # RSI高于超买线，卖出信号
        df.loc[df['rsi'] > overbought, 'signal'] = -1
        
        # RSI从下往上穿越超卖线，更强买入信号
        df.loc[(df['rsi'] > oversold) & (df['rsi'].shift(1) <= oversold), 'signal'] = 1
        # RSI从上往下穿越超买线，更强卖出信号
        df.loc[(df['rsi'] < overbought) & (df['rsi'].shift(1) >= overbought), 'signal'] = -1
        
        return df


class BOLLStrategy(BaseStrategy):
    """布林带突破策略"""
    
    def __init__(self, period: int = 20, std_dev: float = 2.0):
        super().__init__(
            name="布林带策略",
            params={"period": period, "std_dev": std_dev}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 20)
        std_dev = self.params.get('std_dev', 2.0)
        
        # 计算布林带
        df['boll_mid'] = df[close_col].rolling(window=period).mean()
        df['boll_std'] = df[close_col].rolling(window=period).std()
        df['boll_upper'] = df['boll_mid'] + std_dev * df['boll_std']
        df['boll_lower'] = df['boll_mid'] - std_dev * df['boll_std']
        
        # 生成信号
        df['signal'] = 0
        
        # 价格跌破下轨，买入信号（抄底）
        df.loc[df[close_col] < df['boll_lower'], 'signal'] = 1
        # 价格突破上轨，卖出信号（获利了结）
        df.loc[df[close_col] > df['boll_upper'], 'signal'] = -1
        
        # 价格从下轨反弹，更强买入信号
        df.loc[
            (df[close_col] > df['boll_lower']) & 
            (df[close_col].shift(1) <= df['boll_lower'].shift(1)), 
            'signal'
        ] = 1
        
        # 价格从上轨回落，更强卖出信号
        df.loc[
            (df[close_col] < df['boll_upper']) & 
            (df[close_col].shift(1) >= df['boll_upper'].shift(1)), 
            'signal'
        ] = -1
        
        return df


class VolumePriceStrategy(BaseStrategy):
    """量价组合策略"""
    
    def __init__(self, volume_period: int = 5, price_change_threshold: float = 3.0):
        super().__init__(
            name="量价策略",
            params={"volume_period": volume_period, "price_change_threshold": price_change_threshold}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        volume_col = col_map.get('volume', '成交量')
        
        volume_period = self.params.get('volume_period', 5)
        threshold = self.params.get('price_change_threshold', 3.0)
        
        # 计算成交量均线
        df['vol_ma'] = df[volume_col].rolling(window=volume_period).mean()
        
        # 计算价格变化率
        df['price_change'] = df[close_col].pct_change() * 100
        
        # 放量标志（成交量超过均量的1.5倍）
        df['volume_breakout'] = df[volume_col] > df['vol_ma'] * 1.5
        
        # 生成信号
        df['signal'] = 0
        
        # 放量上涨，买入信号
        df.loc[
            (df['volume_breakout']) & 
            (df['price_change'] > threshold), 
            'signal'
        ] = 1
        
        # 放量下跌，卖出信号
        df.loc[
            (df['volume_breakout']) & 
            (df['price_change'] < -threshold), 
            'signal'
        ] = -1
        
        # 缩量创新高，谨慎卖出
        df.loc[
            (df[volume_col] < df['vol_ma'] * 0.5) & 
            (df[close_col] > df[close_col].rolling(window=10).max().shift(1)), 
            'signal'
        ] = -1
        
        return df


# 策略注册表
STRATEGY_REGISTRY = {
    "ma_cross": MAStrategy,
    "macd": MACDStrategy,
    "kdj": KDJStrategy,
    "rsi": RSIStrategy,
    "boll": BOLLStrategy,
    "volume_price": VolumePriceStrategy,
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
        # 创建临时实例获取名称和参数
        try:
            instance = cls()
        except:
            instance = cls(short_period=5, long_period=20)
        strategies.append({
            "id": name,
            "name": instance.name,
            "params": instance.params
        })
    return strategies