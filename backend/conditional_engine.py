#!/usr/bin/env python3
"""
条件依赖引擎
支持：前置条件 → 触发信号 → 执行交易
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import operator
from app.services.backtest import BacktestEngine
from app.services.strategy import get_strategy


class ConditionOp(Enum):
    """条件操作符"""
    GT = ">"      # 大于
    LT = "<"      # 小于
    GE = ">="     # 大于等于
    LE = "<="     # 小于等于
    EQ = "=="     # 等于
    NE = "!="     # 不等于
    IN = "IN"     # 包含
    NOT_IN = "NOT IN"  # 不包含


class LogicOp(Enum):
    """逻辑操作符"""
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


@dataclass
class Condition:
    """条件定义"""
    name: str                           # 条件名称
    field: str                          # 数据字段
    op: ConditionOp                     # 操作符
    value: Any                          # 比较值（可以是数值或字段名）
    description: str = ""               # 描述
    
    def evaluate(self, df: pd.DataFrame) -> np.ndarray:
        """
        评估条件，返回布尔数组
        """
        if self.field not in df.columns:
            return np.zeros(len(df), dtype=bool)
        
        field_values = df[self.field]
        
        # 如果value是字符串且是df中的列名，则使用列值
        if isinstance(self.value, str) and self.value in df.columns:
            compare_values = df[self.value]
        else:
            compare_values = self.value
        
        if self.op == ConditionOp.GT:
            return (field_values > compare_values).values
        elif self.op == ConditionOp.LT:
            return (field_values < compare_values).values
        elif self.op == ConditionOp.GE:
            return (field_values >= compare_values).values
        elif self.op == ConditionOp.LE:
            return (field_values <= compare_values).values
        elif self.op == ConditionOp.EQ:
            return (field_values == compare_values).values
        elif self.op == ConditionOp.NE:
            return (field_values != compare_values).values
        elif self.op == ConditionOp.IN:
            return field_values.isin(self.value).values
        elif self.op == ConditionOp.NOT_IN:
            return ~field_values.isin(self.value).values
        
        return np.zeros(len(df), dtype=bool)


@dataclass
class ConditionGroup:
    """条件组"""
    conditions: List[Condition]         # 条件列表
    logic_op: LogicOp = LogicOp.AND     # 组合逻辑
    
    def evaluate(self, df: pd.DataFrame) -> np.ndarray:
        """
        评估条件组
        """
        if not self.conditions:
            return np.ones(len(df), dtype=bool)
        
        results = [cond.evaluate(df) for cond in self.conditions]
        
        if self.logic_op == LogicOp.AND:
            result = np.all(results, axis=0)
        elif self.logic_op == LogicOp.OR:
            result = np.any(results, axis=0)
        else:
            result = results[0]
        
        return result


@dataclass
class StrategySignal:
    """策略信号"""
    strategy_id: str                    # 策略ID
    params: Dict                        # 策略参数
    
    def generate(self, df: pd.DataFrame) -> np.ndarray:
        """
        生成信号
        """
        strategy = get_strategy(self.strategy_id)
        strategy.params = self.params
        df_signal = strategy.generate_signals(df.copy())
        return df_signal['signal'].values


@dataclass  
class SignalGroup:
    """信号组"""
    signals: List[StrategySignal]       # 信号列表
    logic_op: LogicOp = LogicOp.OR      # 组合逻辑（默认OR）
    
    def generate(self, df: pd.DataFrame) -> np.ndarray:
        """
        生成组合信号
        """
        if not self.signals:
            return np.zeros(len(df))
        
        signals = [sig.generate(df) for sig in self.signals]
        
        if self.logic_op == LogicOp.AND:
            # AND模式：所有策略都发出买入才买入，都发出卖出才卖出
            signals_array = np.array(signals)
            buy = np.all(signals_array == 1, axis=0).astype(int)
            sell = np.all(signals_array == -1, axis=0).astype(int)
            return buy - sell
        elif self.logic_op == LogicOp.OR:
            # OR模式：任一策略发出信号
            signals_array = np.array(signals)
            combined = np.sum(signals_array, axis=0)
            return np.clip(combined, -1, 1)
        
        return signals[0]


@dataclass
class ConditionalStrategy:
    """条件依赖策略"""
    name: str                           # 策略名称
    preconditions: ConditionGroup       # 前置条件组
    signals: SignalGroup                # 信号组
    
    def generate_signals(self, df: pd.DataFrame) -> np.ndarray:
        """
        生成依赖条件的信号
        
        逻辑：
        1. 先评估前置条件
        2. 只有前置条件满足时，才使用策略信号
        3. 前置条件不满足时，信号为0（不交易）
        """
        # 评估前置条件
        precondition_met = self.preconditions.evaluate(df)
        
        # 生成策略信号
        strategy_signals = self.signals.generate(df)
        
        # 条件依赖：只有前置条件满足时才执行信号
        # 买入信号：前置条件满足 AND 策略发出买入
        # 卖出信号：前置条件满足 AND 策略发出卖出（或者止损）
        final_signals = np.where(precondition_met, strategy_signals, 0)
        
        return final_signals
    
    def describe(self) -> str:
        """生成描述"""
        cond_desc = " AND ".join([c.description or c.name for c in self.preconditions.conditions])
        sig_desc = f" {self.signals.logic_op.value} ".join([f"{s.strategy_id}({s.params})" for s in self.signals])
        return f"WHERE {cond_desc} THEN {sig_desc}"


class ConditionalEngine:
    """条件依赖引擎"""
    
    # 预定义条件
    PREDEFINED_CONDITIONS = {
        '市值>50亿': Condition('市值', '市值', ConditionOp.GT, 50e9, '市值>50亿'),
        '市值>100亿': Condition('市值', '市值', ConditionOp.GT, 100e9, '市值>100亿'),
        '非ST': Condition('非ST', '名称', ConditionOp.NOT_IN, ['ST', '*ST'], '非ST股票'),
        '成交量>MA5': Condition('成交量放大', '成交量', ConditionOp.GT, 'MA5成交量', '成交量>5日均量'),
        '价格>MA20': Condition('价格强势', '收盘', ConditionOp.GT, 'MA20', '价格>MA20'),
    }
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化
        
        Args:
            df: 包含OHLCV的DataFrame
        """
        self.df = df
        self.n = len(df)
        
        # 预计算常用指标
        self._precompute_indicators()
    
    def _precompute_indicators(self):
        """预计算技术指标"""
        # 移动平均
        self.df['MA5'] = self.df['收盘'].rolling(5).mean()
        self.df['MA10'] = self.df['收盘'].rolling(10).mean()
        self.df['MA20'] = self.df['收盘'].rolling(20).mean()
        self.df['MA60'] = self.df['收盘'].rolling(60).mean()
        
        # 成交量移动平均
        self.df['MA5成交量'] = self.df['成交量'].rolling(5).mean()
        self.df['MA10成交量'] = self.df['成交量'].rolling(10).mean()
    
    def create_condition(
        self,
        name: str,
        field: str,
        op: str,
        value: Any,
        description: str = ""
    ) -> Condition:
        """
        创建条件
        """
        op_map = {
            '>': ConditionOp.GT,
            '<': ConditionOp.LT,
            '>=': ConditionOp.GE,
            '<=': ConditionOp.LE,
            '==': ConditionOp.EQ,
            '!=': ConditionOp.NE,
        }
        return Condition(name, field, op_map.get(op, ConditionOp.GT), value, description)
    
    def create_conditional_strategy(
        self,
        name: str,
        preconditions: List[Condition],
        signals: List[tuple],  # [(strategy_id, params), ...]
        signal_logic: str = "OR"
    ) -> ConditionalStrategy:
        """
        创建条件依赖策略
        
        Args:
            name: 策略名称
            preconditions: 前置条件列表
            signals: 信号列表 [(strategy_id, params), ...]
            signal_logic: 信号组合逻辑 ("AND" or "OR")
        
        Returns:
            ConditionalStrategy
        """
        cond_group = ConditionGroup(
            conditions=preconditions,
            logic_op=LogicOp.AND
        )
        
        signal_group = SignalGroup(
            signals=[StrategySignal(s[0], s[1]) for s in signals],
            logic_op=LogicOp.OR if signal_logic == "OR" else LogicOp.AND
        )
        
        return ConditionalStrategy(name, cond_group, signal_group)
    
    def run_backtest(self, signal: np.ndarray) -> Dict:
        """
        运行回测
        """
        df_test = self.df.copy()
        df_test['signal'] = signal
        
        engine = BacktestEngine()
        result = engine.run_backtest(df_test)
        
        return result


# ===== 便捷函数 =====

def create_simple_conditional_strategy(
    df: pd.DataFrame,
    strategy_id: str,
    strategy_params: Dict,
    condition_field: str,
    condition_op: str,
    condition_value: Any
) -> ConditionalStrategy:
    """
    创建简单的条件依赖策略
    
    示例：
    create_simple_conditional_strategy(
        df, 'ma_cross', {'short': 10, 'long': 20},
        '成交量', '>', 'MA5成交量'
    )
    """
    engine = ConditionalEngine(df)
    
    condition = engine.create_condition(
        name=f"{condition_field}{condition_op}{condition_value}",
        field=condition_field,
        op=condition_op,
        value=condition_value
    )
    
    return engine.create_conditional_strategy(
        name=f"条件{condition.name}触发{strategy_id}",
        preconditions=[condition],
        signals=[(strategy_id, strategy_params)]
    )


if __name__ == '__main__':
    # 测试
    import pandas as pd
    import numpy as np
    
    # 加载数据
    df = pd.read_csv('data_cache/day/002487_day.csv')
    df['日期'] = pd.to_datetime(df['日期'].astype(str))
    df = df.sort_values('日期').reset_index(drop=True).iloc[-504:].copy()
    
    print(f"数据: {len(df)}条")
    
    # 创建引擎
    engine = ConditionalEngine(df)
    
    # 测试条件
    cond1 = engine.create_condition("成交量放大", "成交量", ">", "MA5成交量")
    result1 = cond1.evaluate(engine.df)
    print(f"成交量>MA5: {np.sum(result1)} 天满足")
    
    # 创建策略
    strategy = engine.create_conditional_strategy(
        name="放量MA金叉",
        preconditions=[cond1],
        signals=[('ma_cross', {'short_period': 10, 'long_period': 20})]
    )
    
    signal = strategy.generate_signals(engine.df)
    result = engine.run_backtest(signal)
    
    print(f"\n策略: {strategy.describe()}")
    print(f"收益: {result['total_return']:.2f}%")
    print(f"夏普: {result['sharpe_ratio']:.2f}")