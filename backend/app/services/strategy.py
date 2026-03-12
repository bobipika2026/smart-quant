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


class CCIStrategy(BaseStrategy):
    """CCI顺势指标策略"""
    
    def __init__(self, period: int = 14, oversold: float = -100, overbought: float = 100):
        super().__init__(
            name="CCI策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', -100)
        overbought = self.params.get('overbought', 100)
        
        # 计算CCI
        tp = (df[high_col] + df[low_col] + df[close_col]) / 3
        ma_tp = tp.rolling(window=period).mean()
        md = tp.rolling(window=period).apply(lambda x: np.abs(x - x.mean()).mean())
        
        df['cci'] = (tp - ma_tp) / (0.015 * md)
        
        # 生成信号
        df['signal'] = 0
        # CCI低于-100，超卖买入
        df.loc[df['cci'] < oversold, 'signal'] = 1
        # CCI高于+100，超买卖出
        df.loc[df['cci'] > overbought, 'signal'] = -1
        
        # CCI从下往上穿越-100，更强买入
        df.loc[
            (df['cci'] > oversold) & (df['cci'].shift(1) <= oversold), 
            'signal'
        ] = 1
        # CCI从上往下穿越+100，更强卖出
        df.loc[
            (df['cci'] < overbought) & (df['cci'].shift(1) >= overbought), 
            'signal'
        ] = -1
        
        return df


class WRStrategy(BaseStrategy):
    """威廉指标策略"""
    
    def __init__(self, period: int = 14, oversold: float = -80, overbought: float = -20):
        super().__init__(
            name="WR策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', -80)
        overbought = self.params.get('overbought', -20)
        
        # 计算威廉指标
        high_n = df[high_col].rolling(window=period).max()
        low_n = df[low_col].rolling(window=period).min()
        
        df['wr'] = (high_n - df[close_col]) / (high_n - low_n) * -100
        
        # 生成信号
        df['signal'] = 0
        # WR低于-80，超卖买入
        df.loc[df['wr'] < oversold, 'signal'] = 1
        # WR高于-20，超买卖出
        df.loc[df['wr'] > overbought, 'signal'] = -1
        
        return df


class OBVStrategy(BaseStrategy):
    """OBV能量潮策略"""
    
    def __init__(self, period: int = 20):
        super().__init__(
            name="OBV策略",
            params={"period": period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        volume_col = col_map.get('volume', '成交量')
        
        period = self.params.get('period', 20)
        
        # 计算OBV
        df['obv'] = 0
        for i in range(1, len(df)):
            if df[close_col].iloc[i] > df[close_col].iloc[i-1]:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] + df[volume_col].iloc[i]
            elif df[close_col].iloc[i] < df[close_col].iloc[i-1]:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1] - df[volume_col].iloc[i]
            else:
                df.loc[df.index[i], 'obv'] = df['obv'].iloc[i-1]
        
        # OBV均线
        df['obv_ma'] = df['obv'].rolling(window=period).mean()
        
        # 生成信号
        df['signal'] = 0
        # OBV上穿均线，买入
        df.loc[df['obv'] > df['obv_ma'], 'signal'] = 1
        # OBV下穿均线，卖出
        df.loc[df['obv'] < df['obv_ma'], 'signal'] = -1
        
        return df


class DMIStrategy(BaseStrategy):
    """DMI动向指标策略"""
    
    def __init__(self, period: int = 14, threshold: float = 25):
        super().__init__(
            name="DMI策略",
            params={"period": period, "threshold": threshold}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        threshold = self.params.get('threshold', 25)
        
        # 计算DMI
        df['up_move'] = df[high_col] - df[high_col].shift(1)
        df['down_move'] = df[low_col].shift(1) - df[low_col]
        
        df['plus_dm'] = np.where(
            (df['up_move'] > df['down_move']) & (df['up_move'] > 0),
            df['up_move'], 0
        )
        df['minus_dm'] = np.where(
            (df['down_move'] > df['up_move']) & (df['down_move'] > 0),
            df['down_move'], 0
        )
        
        df['plus_di'] = 100 * df['plus_dm'].rolling(window=period).mean() / df[close_col].rolling(window=period).mean()
        df['minus_di'] = 100 * df['minus_dm'].rolling(window=period).mean() / df[close_col].rolling(window=period).mean()
        
        # 生成信号
        df['signal'] = 0
        # +DI上穿-DI，且+DI大于阈值，买入
        df.loc[
            (df['plus_di'] > df['minus_di']) & 
            (df['plus_di'].shift(1) <= df['minus_di'].shift(1)) &
            (df['plus_di'] > threshold),
            'signal'
        ] = 1
        # -DI上穿+DI，且-DI大于阈值，卖出
        df.loc[
            (df['minus_di'] > df['plus_di']) & 
            (df['minus_di'].shift(1) <= df['plus_di'].shift(1)) &
            (df['minus_di'] > threshold),
            'signal'
        ] = -1
        
        return df


class BSIAStrategy(BaseStrategy):
    """BIAS乖离率策略"""
    
    def __init__(self, period: int = 20, oversold: float = -10, overbought: float = 10):
        super().__init__(
            name="BIAS策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 20)
        oversold = self.params.get('oversold', -10)
        overbought = self.params.get('overbought', 10)
        
        # 计算乖离率
        ma = df[close_col].rolling(window=period).mean()
        df['bias'] = (df[close_col] - ma) / ma * 100
        
        # 生成信号
        df['signal'] = 0
        # 乖离率低于-10%，超卖买入
        df.loc[df['bias'] < oversold, 'signal'] = 1
        # 乖离率高于+10%，超买卖出
        df.loc[df['bias'] > overbought, 'signal'] = -1
        
        return df


class ATRStrategy(BaseStrategy):
    """ATR真实波幅策略"""
    
    def __init__(self, period: int = 14, multiplier: float = 2.0):
        super().__init__(
            name="ATR策略",
            params={"period": period, "multiplier": multiplier}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        multiplier = self.params.get('multiplier', 2.0)
        
        # 计算TR
        df['tr'] = np.maximum(
            df[high_col] - df[low_col],
            np.maximum(
                np.abs(df[high_col] - df[close_col].shift(1)),
                np.abs(df[low_col] - df[close_col].shift(1))
            )
        )
        
        # 计算ATR
        df['atr'] = df['tr'].rolling(window=period).mean()
        
        # 计算上下轨
        df['upper'] = df[close_col] + multiplier * df['atr']
        df['lower'] = df[close_col] - multiplier * df['atr']
        
        # 生成信号
        df['signal'] = 0
        # 价格突破下轨，买入
        df.loc[df[close_col] < df['lower'], 'signal'] = 1
        # 价格突破上轨，卖出
        df.loc[df[close_col] > df['upper'], 'signal'] = -1
        
        return df


class SARStrategy(BaseStrategy):
    """SAR抛物线指标策略"""
    
    def __init__(self, af_start: float = 0.02, af_increment: float = 0.02, af_max: float = 0.2):
        super().__init__(
            name="SAR策略",
            params={"af_start": af_start, "af_increment": af_increment, "af_max": af_max}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        # 简化版SAR计算
        df['sar'] = df[close_col].rolling(window=5).mean()
        
        # 判断趋势
        df['trend'] = 0
        df.loc[df[close_col] > df['sar'], 'trend'] = 1  # 上升趋势
        df.loc[df[close_col] < df['sar'], 'trend'] = -1  # 下降趋势
        
        # 生成信号
        df['signal'] = 0
        # 趋势从下降转为上升，买入
        df.loc[(df['trend'] == 1) & (df['trend'].shift(1) == -1), 'signal'] = 1
        # 趋势从上升转为下降，卖出
        df.loc[(df['trend'] == -1) & (df['trend'].shift(1) == 1), 'signal'] = -1
        
        return df


class AroonStrategy(BaseStrategy):
    """Aroon阿隆指标策略"""
    
    def __init__(self, period: int = 14, threshold: float = 70):
        super().__init__(
            name="Aroon策略",
            params={"period": period, "threshold": threshold}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        
        period = self.params.get('period', 14)
        threshold = self.params.get('threshold', 70)
        
        # 计算Aroon
        df['aroon_up'] = df[high_col].rolling(window=period).apply(
            lambda x: (period - 1 - np.argmax(x)) / (period - 1) * 100, raw=False
        )
        df['aroon_down'] = df[low_col].rolling(window=period).apply(
            lambda x: (period - 1 - np.argmin(x)) / (period - 1) * 100, raw=False
        )
        
        # 生成信号
        df['signal'] = 0
        # Aroon-Up上穿Aroon-Down，买入
        df.loc[(df['aroon_up'] > df['aroon_down']) & 
               (df['aroon_up'].shift(1) <= df['aroon_down'].shift(1)), 'signal'] = 1
        # Aroon-Down上穿Aroon-Up，卖出
        df.loc[(df['aroon_down'] > df['aroon_up']) & 
               (df['aroon_down'].shift(1) <= df['aroon_up'].shift(1)), 'signal'] = -1
        
        return df


class MOMStrategy(BaseStrategy):
    """MOM动量指标策略"""
    
    def __init__(self, period: int = 10, oversold: float = -5, overbought: float = 5):
        super().__init__(
            name="MOM策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 10)
        oversold = self.params.get('oversold', -5)
        overbought = self.params.get('overbought', 5)
        
        # 计算动量
        df['mom'] = df[close_col] - df[close_col].shift(period)
        
        # 生成信号
        df['signal'] = 0
        # 动量由负转正，买入
        df.loc[(df['mom'] > 0) & (df['mom'].shift(1) <= 0), 'signal'] = 1
        # 动量由正转负，卖出
        df.loc[(df['mom'] < 0) & (df['mom'].shift(1) >= 0), 'signal'] = -1
        
        return df


class ROCStrategy(BaseStrategy):
    """ROC变动率指标策略"""
    
    def __init__(self, period: int = 12, oversold: float = -10, overbought: float = 10):
        super().__init__(
            name="ROC策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 12)
        oversold = self.params.get('oversold', -10)
        overbought = self.params.get('overbought', 10)
        
        # 计算ROC
        df['roc'] = (df[close_col] - df[close_col].shift(period)) / df[close_col].shift(period) * 100
        
        # 生成信号
        df['signal'] = 0
        # ROC低于超卖线，买入
        df.loc[df['roc'] < oversold, 'signal'] = 1
        # ROC高于超买线，卖出
        df.loc[df['roc'] > overbought, 'signal'] = -1
        
        return df


class VWAPStrategy(BaseStrategy):
    """VWAP成交量加权平均价策略"""
    
    def __init__(self, deviation: float = 0.02):
        super().__init__(
            name="VWAP策略",
            params={"deviation": deviation}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        volume_col = col_map.get('volume', '成交量')
        
        deviation = self.params.get('deviation', 0.02)
        
        # 计算典型价格
        typical_price = (df[high_col] + df[low_col] + df[close_col]) / 3
        
        # 计算VWAP
        df['vwap'] = (typical_price * df[volume_col]).cumsum() / df[volume_col].cumsum()
        
        # 生成信号
        df['signal'] = 0
        # 价格低于VWAP一定比例，买入
        df.loc[df[close_col] < df['vwap'] * (1 - deviation), 'signal'] = 1
        # 价格高于VWAP一定比例，卖出
        df.loc[df[close_col] > df['vwap'] * (1 + deviation), 'signal'] = -1
        
        return df


class DonchianStrategy(BaseStrategy):
    """唐奇安通道策略"""
    
    def __init__(self, period: int = 20):
        super().__init__(
            name="唐奇安通道",
            params={"period": period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 20)
        
        # 计算唐奇安通道
        df['upper'] = df[high_col].rolling(window=period).max()
        df['lower'] = df[low_col].rolling(window=period).min()
        df['mid'] = (df['upper'] + df['lower']) / 2
        
        # 生成信号
        df['signal'] = 0
        # 突破上轨，买入
        df.loc[df[close_col] > df['upper'].shift(1), 'signal'] = 1
        # 跌破下轨，卖出
        df.loc[df[close_col] < df['lower'].shift(1), 'signal'] = -1
        
        return df


class KeltnerStrategy(BaseStrategy):
    """肯特纳通道策略"""
    
    def __init__(self, period: int = 20, multiplier: float = 2.0):
        super().__init__(
            name="肯特纳通道",
            params={"period": period, "multiplier": multiplier}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 20)
        multiplier = self.params.get('multiplier', 2.0)
        
        # 计算ATR
        tr = np.maximum(
            df[high_col] - df[low_col],
            np.maximum(
                np.abs(df[high_col] - df[close_col].shift(1)),
                np.abs(df[low_col] - df[close_col].shift(1))
            )
        )
        atr = pd.Series(tr).rolling(window=period).mean()
        
        # 计算中轨（EMA）
        mid = df[close_col].ewm(span=period).mean()
        
        # 计算上下轨
        df['upper'] = mid + multiplier * atr
        df['lower'] = mid - multiplier * atr
        
        # 生成信号
        df['signal'] = 0
        # 突破上轨，买入
        df.loc[df[close_col] > df['upper'], 'signal'] = 1
        # 跌破下轨，卖出
        df.loc[df[close_col] < df['lower'], 'signal'] = -1
        
        return df


class MFIStrategy(BaseStrategy):
    """MFI资金流量指标策略"""
    
    def __init__(self, period: int = 14, oversold: float = 20, overbought: float = 80):
        super().__init__(
            name="MFI策略",
            params={"period": period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        volume_col = col_map.get('volume', '成交量')
        
        period = self.params.get('period', 14)
        oversold = self.params.get('oversold', 20)
        overbought = self.params.get('overbought', 80)
        
        # 计算典型价格
        typical_price = (df[high_col] + df[low_col] + df[close_col]) / 3
        
        # 计算资金流量
        mf = typical_price * df[volume_col]
        
        # 计算正负资金流量
        positive_mf = np.where(typical_price > typical_price.shift(1), mf, 0)
        negative_mf = np.where(typical_price < typical_price.shift(1), mf, 0)
        
        # 计算MFI
        positive_sum = pd.Series(positive_mf).rolling(window=period).sum()
        negative_sum = pd.Series(negative_mf).rolling(window=period).sum()
        
        df['mfi'] = 100 - (100 / (1 + positive_sum / negative_sum))
        
        # 生成信号
        df['signal'] = 0
        # MFI低于超卖线，买入
        df.loc[df['mfi'] < oversold, 'signal'] = 1
        # MFI高于超买线，卖出
        df.loc[df['mfi'] > overbought, 'signal'] = -1
        
        return df


class StochasticStrategy(BaseStrategy):
    """Stochastic随机指标策略"""
    
    def __init__(self, k_period: int = 14, d_period: int = 3, oversold: float = 20, overbought: float = 80):
        super().__init__(
            name="Stochastic策略",
            params={"k_period": k_period, "d_period": d_period, "oversold": oversold, "overbought": overbought}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        k_period = self.params.get('k_period', 14)
        d_period = self.params.get('d_period', 3)
        oversold = self.params.get('oversold', 20)
        overbought = self.params.get('overbought', 80)
        
        # 计算Stochastic
        low_min = df[low_col].rolling(window=k_period).min()
        high_max = df[high_col].rolling(window=k_period).max()
        
        df['stoch_k'] = (df[close_col] - low_min) / (high_max - low_min) * 100
        df['stoch_d'] = df['stoch_k'].rolling(window=d_period).mean()
        
        # 生成信号
        df['signal'] = 0
        # K线上穿D线且低于超卖线，买入
        df.loc[
            (df['stoch_k'] > df['stoch_d']) & 
            (df['stoch_k'].shift(1) <= df['stoch_d'].shift(1)) &
            (df['stoch_k'] < oversold),
            'signal'
        ] = 1
        # K线下穿D线且高于超买线，卖出
        df.loc[
            (df['stoch_k'] < df['stoch_d']) & 
            (df['stoch_k'].shift(1) >= df['stoch_d'].shift(1)) &
            (df['stoch_k'] > overbought),
            'signal'
        ] = -1
        
        return df


class TRIXStrategy(BaseStrategy):
    """TRIX三重指数平滑策略"""
    
    def __init__(self, period: int = 14):
        super().__init__(
            name="TRIX策略",
            params={"period": period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        
        # 计算TRIX
        ema1 = df[close_col].ewm(span=period).mean()
        ema2 = ema1.ewm(span=period).mean()
        ema3 = ema2.ewm(span=period).mean()
        
        df['trix'] = (ema3 - ema3.shift(1)) / ema3.shift(1) * 100
        df['trix_signal'] = df['trix'].ewm(span=period).mean()
        
        # 生成信号
        df['signal'] = 0
        # TRIX上穿信号线，买入
        df.loc[(df['trix'] > df['trix_signal']) & 
               (df['trix'].shift(1) <= df['trix_signal'].shift(1)), 'signal'] = 1
        # TRIX下穿信号线，卖出
        df.loc[(df['trix'] < df['trix_signal']) & 
               (df['trix'].shift(1) >= df['trix_signal'].shift(1)), 'signal'] = -1
        
        return df


class ADXStrategy(BaseStrategy):
    """ADX平均趋向指数策略"""
    
    def __init__(self, period: int = 14, threshold: float = 25):
        super().__init__(
            name="ADX策略",
            params={"period": period, "threshold": threshold}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        period = self.params.get('period', 14)
        threshold = self.params.get('threshold', 25)
        
        # 计算+DM和-DM
        df['up_move'] = df[high_col] - df[high_col].shift(1)
        df['down_move'] = df[low_col].shift(1) - df[low_col]
        
        df['plus_dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
        df['minus_dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)
        
        # 计算TR
        df['tr'] = np.maximum(
            df[high_col] - df[low_col],
            np.maximum(
                np.abs(df[high_col] - df[close_col].shift(1)),
                np.abs(df[low_col] - df[close_col].shift(1))
            )
        )
        
        # 平滑处理
        atr = df['tr'].rolling(window=period).mean()
        plus_di = 100 * df['plus_dm'].rolling(window=period).mean() / atr
        minus_di = 100 * df['minus_dm'].rolling(window=period).mean() / atr
        
        # 计算DX和ADX
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        df['adx'] = dx.rolling(window=period).mean()
        df['plus_di'] = plus_di
        df['minus_di'] = minus_di
        
        # 生成信号
        df['signal'] = 0
        # ADX大于阈值且+DI>-DI，买入
        df.loc[(df['adx'] > threshold) & (df['plus_di'] > df['minus_di']), 'signal'] = 1
        # ADX大于阈值且-DI>+DI，卖出
        df.loc[(df['adx'] > threshold) & (df['minus_di'] > df['plus_di']), 'signal'] = -1
        
        return df


class BBIStrategy(BaseStrategy):
    """BBI多空指标策略"""
    
    def __init__(self, periods: str = "3,6,12,24"):
        super().__init__(
            name="BBI策略",
            params={"periods": periods}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        periods = [int(p) for p in self.params.get('periods', "3,6,12,24").split(',')]
        
        # 计算BBI
        ma_sum = sum(df[close_col].rolling(window=p).mean() for p in periods)
        df['bbi'] = ma_sum / len(periods)
        
        # 生成信号
        df['signal'] = 0
        # 价格上穿BBI，买入
        df.loc[(df[close_col] > df['bbi']) & (df[close_col].shift(1) <= df['bbi'].shift(1)), 'signal'] = 1
        # 价格下穿BBI，卖出
        df.loc[(df[close_col] < df['bbi']) & (df[close_col].shift(1) >= df['bbi'].shift(1)), 'signal'] = -1
        
        return df


class EXPMAtrategy(BaseStrategy):
    """EXPMA指数平均数策略"""
    
    def __init__(self, short_period: int = 12, long_period: int = 50):
        super().__init__(
            name="EXPMA策略",
            params={"short_period": short_period, "long_period": long_period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        short_period = self.params.get('short_period', 12)
        long_period = self.params.get('long_period', 50)
        
        # 计算EXPMA
        df['expma_short'] = df[close_col].ewm(span=short_period).mean()
        df['expma_long'] = df[close_col].ewm(span=long_period).mean()
        
        # 生成信号
        df['signal'] = 0
        # 短期EXPMA上穿长期EXPMA，买入
        df.loc[(df['expma_short'] > df['expma_long']) & 
               (df['expma_short'].shift(1) <= df['expma_long'].shift(1)), 'signal'] = 1
        # 短期EXPMA下穿长期EXPMA，卖出
        df.loc[(df['expma_short'] < df['expma_long']) & 
               (df['expma_short'].shift(1) >= df['expma_long'].shift(1)), 'signal'] = -1
        
        return df


class IchimokuStrategy(BaseStrategy):
    """一目均衡表策略"""
    
    def __init__(self, tenkan: int = 9, kijun: int = 26, senkou: int = 52):
        super().__init__(
            name="一目均衡表",
            params={"tenkan": tenkan, "kijun": kijun, "senkou": senkou}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        high_col = col_map.get('high', '最高')
        low_col = col_map.get('low', '最低')
        close_col = col_map.get('close', '收盘')
        
        tenkan = self.params.get('tenkan', 9)
        kijun = self.params.get('kijun', 26)
        
        # 计算转换线
        tenkan_high = df[high_col].rolling(window=tenkan).max()
        tenkan_low = df[low_col].rolling(window=tenkan).min()
        df['tenkan'] = (tenkan_high + tenkan_low) / 2
        
        # 计算基准线
        kijun_high = df[high_col].rolling(window=kijun).max()
        kijun_low = df[low_col].rolling(window=kijun).min()
        df['kijun'] = (kijun_high + kijun_low) / 2
        
        # 生成信号
        df['signal'] = 0
        # 价格上穿转换线和基准线，买入
        df.loc[(df[close_col] > df['tenkan']) & (df[close_col] > df['kijun']), 'signal'] = 1
        # 价格下穿转换线和基准线，卖出
        df.loc[(df[close_col] < df['tenkan']) & (df[close_col] < df['kijun']), 'signal'] = -1
        
        return df


class ZigZagStrategy(BaseStrategy):
    """ZigZag之字形策略"""
    
    def __init__(self, threshold: float = 5.0):
        super().__init__(
            name="ZigZag策略",
            params={"threshold": threshold}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        threshold = self.params.get('threshold', 5.0)
        
        # 简化版ZigZag
        df['change'] = df[close_col].pct_change() * 100
        
        # 计算累积变化
        df['zigzag'] = 0
        trend = 0
        for i in range(1, len(df)):
            if df['change'].iloc[i] > threshold:
                trend = 1
            elif df['change'].iloc[i] < -threshold:
                trend = -1
            
            if trend == 1:
                df.loc[df.index[i], 'zigzag'] = 1
            elif trend == -1:
                df.loc[df.index[i], 'zigzag'] = -1
            else:
                df.loc[df.index[i], 'zigzag'] = df['zigzag'].iloc[i-1]
        
        # 生成信号
        df['signal'] = 0
        df.loc[df['zigzag'] == 1, 'signal'] = 1
        df.loc[df['zigzag'] == -1, 'signal'] = -1
        
        return df


class DEMAStrategy(BaseStrategy):
    """DEMA双指数移动平均策略"""
    
    def __init__(self, short_period: int = 10, long_period: int = 30):
        super().__init__(
            name="DEMA策略",
            params={"short_period": short_period, "long_period": long_period}
        )
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        col_map = {c.lower(): c for c in df.columns}
        close_col = col_map.get('close', '收盘')
        
        short_period = self.params.get('short_period', 10)
        long_period = self.params.get('long_period', 30)
        
        # 计算DEMA
        def dema(data, period):
            ema1 = data.ewm(span=period).mean()
            ema2 = ema1.ewm(span=period).mean()
            return 2 * ema1 - ema2
        
        df['dema_short'] = dema(df[close_col], short_period)
        df['dema_long'] = dema(df[close_col], long_period)
        
        # 生成信号
        df['signal'] = 0
        df.loc[(df['dema_short'] > df['dema_long']) & 
               (df['dema_short'].shift(1) <= df['dema_long'].shift(1)), 'signal'] = 1
        df.loc[(df['dema_short'] < df['dema_long']) & 
               (df['dema_short'].shift(1) >= df['dema_long'].shift(1)), 'signal'] = -1
        
        return df


# 策略注册表
STRATEGY_REGISTRY = {
    "ma_cross": MAStrategy,
    "macd": MACDStrategy,
    "kdj": KDJStrategy,
    "rsi": RSIStrategy,
    "boll": BOLLStrategy,
    "volume_price": VolumePriceStrategy,
    "cci": CCIStrategy,
    "wr": WRStrategy,
    "obv": OBVStrategy,
    "dmi": DMIStrategy,
    "bias": BSIAStrategy,
    "atr": ATRStrategy,
    "sar": SARStrategy,
    "aroon": AroonStrategy,
    "mom": MOMStrategy,
    "roc": ROCStrategy,
    "vwap": VWAPStrategy,
    "donchian": DonchianStrategy,
    "keltner": KeltnerStrategy,
    "mfi": MFIStrategy,
    "stochastic": StochasticStrategy,
    "trix": TRIXStrategy,
    "adx": ADXStrategy,
    "bbi": BBIStrategy,
    "expma": EXPMAtrategy,
    "ichimoku": IchimokuStrategy,
    "zigzag": ZigZagStrategy,
    "dema": DEMAStrategy,
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