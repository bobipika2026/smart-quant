#!/usr/bin/env python3
"""
多因子组合引擎
支持：2因子、3因子、多因子 + 条件因子
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import itertools
from app.services.backtest import BacktestEngine
from app.services.strategy import get_strategy


class CombineMode(Enum):
    AND = "AND"
    OR = "OR"


@dataclass
class FactorConfig:
    """因子配置"""
    code: str           # 因子代码，如 'ma_10_20'
    name: str           # 显示名称
    strategy_id: str    # 策略ID
    params: Dict        # 策略参数
    factor_type: str    # 'strategy' | 'condition' | 'time'


@dataclass
class FactorCombination:
    """因子组合"""
    factors: List[FactorConfig]
    combine_ops: List[CombineMode]  # 组合操作符（长度 = len(factors) - 1）
    
    def describe(self) -> str:
        """生成描述字符串"""
        if len(self.factors) == 1:
            return self.factors[0].name
        
        result = self.factors[0].name
        for i, op in enumerate(self.combine_ops):
            result += f" {op.value} {self.factors[i+1].name}"
        return result


class MultiFactorEngine:
    """多因子组合引擎"""
    
    # 策略因子配置
    STRATEGY_FACTORS = {
        'ma_5_20': ('ma_cross', {'short_period': 5, 'long_period': 20}),
        'ma_5_30': ('ma_cross', {'short_period': 5, 'long_period': 30}),
        'ma_10_20': ('ma_cross', {'short_period': 10, 'long_period': 20}),
        'ma_10_30': ('ma_cross', {'short_period': 10, 'long_period': 30}),
        'macd_default': ('macd', {'fast': 12, 'slow': 26, 'signal': 9}),
        'macd_fast': ('macd', {'fast': 8, 'slow': 17, 'signal': 9}),
        'rsi_14_70': ('rsi', {'period': 14, 'overbought': 70, 'oversold': 30}),
        'rsi_14_80': ('rsi', {'period': 14, 'overbought': 80, 'oversold': 20}),
        'kdj_default': ('kdj', {'n': 9, 'm1': 3, 'm2': 3}),
        'boll_20_2': ('boll', {'period': 20, 'std_dev': 2}),
        'cci_14': ('cci', {'period': 14}),
        'wr_14': ('wr', {'period': 14}),
    }
    
    # 条件因子配置
    CONDITION_FACTORS = {
        'volume_ma5': '成交量 > 5日均量',
        'volume_ma10': '成交量 > 10日均量',
        'cap_gt_50': '市值 > 50亿',
        'cap_gt_100': '市值 > 100亿',
        'north_inflow': '北向资金净流入',
        'not_st': '非ST股票',
        'price_gt_ma20': '价格 > MA20',
    }
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化
        
        Args:
            df: 包含日期和收盘价的DataFrame
        """
        self.df = df
        self.n = len(df)
        self.signals_cache = {}
    
    def generate_signal(self, factor_code: str) -> np.ndarray:
        """
        生成单个因子的信号
        """
        if factor_code in self.signals_cache:
            return self.signals_cache[factor_code]
        
        if factor_code in self.STRATEGY_FACTORS:
            strategy_id, params = self.STRATEGY_FACTORS[factor_code]
            strategy = get_strategy(strategy_id)
            strategy.params = params
            df_signal = strategy.generate_signals(self.df.copy())
            signal = df_signal['signal'].values
        elif factor_code in self.CONDITION_FACTORS:
            signal = self._generate_condition_signal(factor_code)
        else:
            signal = np.zeros(self.n)
        
        self.signals_cache[factor_code] = signal
        return signal
    
    def _generate_condition_signal(self, condition_code: str) -> np.ndarray:
        """
        生成条件因子信号
        """
        signal = np.zeros(self.n)
        
        if condition_code == 'volume_ma5':
            # 成交量 > 5日均量
            if '成交量' in self.df.columns:
                vol_ma5 = self.df['成交量'].rolling(5).mean()
                signal = (self.df['成交量'] > vol_ma5).astype(int).values
        
        elif condition_code == 'volume_ma10':
            # 成交量 > 10日均量
            if '成交量' in self.df.columns:
                vol_ma10 = self.df['成交量'].rolling(10).mean()
                signal = (self.df['成交量'] > vol_ma10).astype(int).values
        
        elif condition_code == 'price_gt_ma20':
            # 价格 > MA20
            ma20 = self.df['收盘'].rolling(20).mean()
            signal = (self.df['收盘'] > ma20).astype(int).values
        
        return signal
    
    def combine_signals(self, signals: List[np.ndarray], ops: List[CombineMode]) -> np.ndarray:
        """
        组合多个信号
        
        Args:
            signals: 信号列表
            ops: 操作符列表
        
        Returns:
            组合后的信号
        """
        if len(signals) == 0:
            return np.zeros(self.n)
        
        if len(signals) == 1:
            return signals[0]
        
        # 从左到右依次组合
        result = signals[0].copy()
        
        for i, op in enumerate(ops):
            next_signal = signals[i + 1]
            
            if op == CombineMode.AND:
                # AND: 都为1才为1，都为-1才为-1
                buy = (result == 1) & (next_signal == 1)
                sell = (result == -1) & (next_signal == -1)
                result = np.where(buy, 1, np.where(sell, -1, 0))
            
            elif op == CombineMode.OR:
                # OR: 任一为1就为1，任一为-1就为-1
                buy = (result == 1) | (next_signal == 1)
                sell = (result == -1) | (next_signal == -1)
                result = np.where(sell, -1, np.where(buy, 1, 0))
        
        return result
    
    def generate_all_combinations(
        self,
        factor_codes: List[str],
        max_factors: int = 3,
        modes: List[CombineMode] = None
    ) -> List[Tuple[List[str], List[CombineMode], str]]:
        """
        生成所有可能的因子组合
        
        Args:
            factor_codes: 因子代码列表
            max_factors: 最大因子数量
            modes: 支持的组合模式
        
        Returns:
            [(因子列表, 操作符列表, 描述)]
        """
        if modes is None:
            modes = [CombineMode.AND, CombineMode.OR]
        
        combinations = []
        
        # 单因子
        for code in factor_codes:
            combinations.append(([code], [], code))
        
        # 2因子组合
        for f1, f2 in itertools.combinations(factor_codes, 2):
            for mode in modes:
                ops = [mode]
                desc = f"{f1} {mode.value} {f2}"
                combinations.append(([f1, f2], ops, desc))
        
        # 3因子组合
        if max_factors >= 3:
            for f1, f2, f3 in itertools.combinations(factor_codes, 3):
                # 所有操作符组合
                for op1, op2 in itertools.product(modes, modes):
                    ops = [op1, op2]
                    desc = f"{f1} {op1.value} {f2} {op2.value} {f3}"
                    combinations.append(([f1, f2, f3], ops, desc))
        
        return combinations
    
    def run_backtest(self, signal: np.ndarray) -> Dict:
        """
        运行回测
        """
        df_test = self.df.copy()
        df_test['signal'] = signal
        
        engine = BacktestEngine()
        result = engine.run_backtest(df_test)
        
        return {
            'total_return': result.get('total_return', 0),
            'annual_return': result.get('annual_return'),
            'sharpe_ratio': result.get('sharpe_ratio', 0),
            'max_drawdown': result.get('max_drawdown'),
            'win_rate': result.get('win_rate'),
            'profit_loss_ratio': result.get('profit_loss_ratio'),
            'trade_count': result.get('trade_count', 0)
        }


async def run_multi_factor_backtest(
    stock_code: str,
    factor_codes: List[str] = None,
    max_factors: int = 3,
    days: int = None
) -> Dict:
    """
    运行多因子回测
    
    Args:
        stock_code: 股票代码
        factor_codes: 因子代码列表（默认使用所有策略因子）
        max_factors: 最大因子数量（1/2/3）
        days: 数据天数（None表示使用全部）
    
    Returns:
        回测结果
    """
    import pandas as pd
    
    # 加载数据
    df = pd.read_csv(f'data_cache/day/{stock_code}_day.csv')
    df['日期'] = pd.to_datetime(df['日期'].astype(str))
    df = df.sort_values('日期').reset_index(drop=True)
    
    # 截取最近N天
    if days:
        start_idx = max(0, len(df) - days)
        df = df.iloc[start_idx:].copy()
    
    # 默认使用所有策略因子
    if factor_codes is None:
        factor_codes = list(MultiFactorEngine.STRATEGY_FACTORS.keys())
    
    # 初始化引擎
    engine = MultiFactorEngine(df)
    
    # 生成所有组合
    combinations = engine.generate_all_combinations(factor_codes, max_factors)
    
    print(f"[多因子回测] 股票: {stock_code}")
    print(f"[多因子回测] 数据: {len(df)}条")
    print(f"[多因子回测] 因子: {len(factor_codes)}个")
    print(f"[多因子回测] 组合: {len(combinations)}个")
    
    results = []
    
    for factors, ops, desc in combinations:
        # 生成信号
        signals = [engine.generate_signal(f) for f in factors]
        
        # 组合信号
        combined = engine.combine_signals(signals, ops)
        
        # 回测
        result = engine.run_backtest(combined)
        
        results.append({
            'factors': factors,
            'ops': [op.value for op in ops],
            'desc': desc,
            **result
        })
    
    # 按收益排序
    results.sort(key=lambda x: x.get('total_return', 0), reverse=True)
    
    # Top 10
    print(f"\n[多因子回测] Top 10 组合:")
    for i, r in enumerate(results[:10]):
        print(f"  #{i+1} {r['desc']}: {r['total_return']:.2f}% (夏普: {r['sharpe_ratio']:.2f})")
    
    return {
        'stock_code': stock_code,
        'total_experiments': len(results),
        'top_results': results[:10],
        'all_results': results
    }


if __name__ == '__main__':
    import asyncio
    
    # 测试
    async def test():
        result = await run_multi_factor_backtest(
            stock_code='002487',
            max_factors=3,
            days=504
        )
        
        print(f"\n完成 {result['total_experiments']} 个实验")
    
    asyncio.run(test())