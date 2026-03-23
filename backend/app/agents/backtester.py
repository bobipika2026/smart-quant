"""
Strategy Backtester Agent - 策略回测师智能体

职责：
1. 执行策略回测
2. 生成回测报告
3. 对比多种策略表现
"""
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime

from app.services.backtest import BacktestEngine
from app.services.data import DataService
from app.services.strategy import get_strategy


class StrategyBacktesterAgent:
    """策略回测师智能体"""
    
    AGENT_NAME = "StrategyBacktester"
    
    # 可用策略列表
    AVAILABLE_STRATEGIES = [
        'ma_cross',      # 均线交叉
        'macd',          # MACD策略
        'rsi',           # RSI策略
        'kdj',           # KDJ策略
        'bollinger',     # 布林带策略
        'factor_score',  # 因子得分策略
    ]
    
    def __init__(self, llm_client=None, config: Dict = None):
        self.llm_client = llm_client
        self.config = config or {}
        self.backtest_engine = BacktestEngine()
    
    def backtest(self, stock_code: str, strategy_name: str = 'factor_score',
                 start_date: str = None, end_date: str = None) -> Dict:
        """
        执行单策略回测
        
        Args:
            stock_code: 股票代码
            strategy_name: 策略名称
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            回测结果
        """
        # 1. 获取数据
        df = DataService.get_cached_data(stock_code, 'day')
        
        if df.empty:
            return {
                'status': 'error',
                'message': f'无法获取 {stock_code} 的数据'
            }
        
        # 过滤日期范围
        if start_date:
            df = df[df['trade_date'] >= start_date]
        if end_date:
            df = df[df['trade_date'] <= end_date]
        
        # 简化信号生成
        df['signal'] = 0
        if 'close' in df.columns:
            # 简单均线策略
            df['ma_short'] = df['close'].rolling(20).mean()
            df['ma_long'] = df['close'].rolling(60).mean()
            df.loc[df['ma_short'] > df['ma_long'], 'signal'] = 1
            df.loc[df['ma_short'] < df['ma_long'], 'signal'] = -1
        
        # 2. 执行回测
        result = self.backtest_engine.run_backtest(df, signal_col='signal')
        
        # 3. 生成报告
        report = self._generate_report(stock_code, strategy_name, result)
        
        return {
            'strategy_name': strategy_name,
            'stock_code': stock_code,
            'total_return': result.get('total_return', 0),
            'annual_return': result.get('annual_return', 0),
            'sharpe_ratio': result.get('sharpe_ratio', 0),
            'max_drawdown': result.get('max_drawdown', 0),
            'win_rate': result.get('win_rate', 0),
            'trade_count': result.get('trade_count', 0),
            'benchmark_return': result.get('benchmark_return', 0),
            'excess_return': result.get('excess_return', 0),
            'backtest_report': report,
        }
    
    def backtest_multi_strategies(self, stock_code: str, 
                                   strategies: List[str] = None) -> List[Dict]:
        """
        多策略回测对比
        
        Args:
            stock_code: 股票代码
            strategies: 策略列表（默认使用所有策略）
        
        Returns:
            各策略回测结果列表
        """
        if strategies is None:
            strategies = self.AVAILABLE_STRATEGIES
        
        results = []
        for strategy in strategies:
            result = self.backtest(stock_code, strategy)
            result['strategy_name'] = strategy
            results.append(result)
        
        # 按收益排序
        results.sort(key=lambda x: x.get('annual_return', 0), reverse=True)
        
        return results
    
    def compare_with_benchmark(self, backtest_result: Dict) -> Dict:
        """与基准对比"""
        comparison = {
            'strategy_return': backtest_result.get('total_return', 0),
            'benchmark_return': backtest_result.get('benchmark_return', 0),
            'excess_return': backtest_result.get('excess_return', 0),
            'outperformed': backtest_result.get('excess_return', 0) > 0,
        }
        return comparison
    
    def _generate_report(self, stock_code: str, strategy_name: str, 
                         result: Dict) -> str:
        """生成回测报告"""
        report = f"""
## 回测报告 - {stock_code}

**策略**: {strategy_name}
**回测日期**: {datetime.now().strftime('%Y-%m-%d')}

### 收益指标

| 指标 | 数值 |
|------|------|
| 总收益 | {result.get('total_return', 0)*100:.2f}% |
| 年化收益 | {result.get('annual_return', 0)*100:.2f}% |
| 基准收益 | {result.get('benchmark_return', 0)*100:.2f}% |
| 超额收益 | {result.get('excess_return', 0)*100:.2f}% |

### 风险指标

| 指标 | 数值 |
|------|------|
| 夏普比率 | {result.get('sharpe_ratio', 0):.3f} |
| 最大回撤 | {result.get('max_drawdown', 0)*100:.2f}% |
| 胜率 | {result.get('win_rate', 0)*100:.1f}% |

### 交易统计

| 指标 | 数值 |
|------|------|
| 交易次数 | {result.get('trade_count', 0)} |
| 盈利次数 | {result.get('win_count', 0)} |
| 亏损次数 | {result.get('loss_count', 0)} |

### 评价

"""
        
        sharpe = result.get('sharpe_ratio', 0)
        if sharpe >= 1.5:
            report += "✅ 夏普比率优秀，风险调整后收益表现良好\n"
        elif sharpe >= 0.5:
            report += "⚠️ 夏普比率一般，需要优化策略\n"
        else:
            report += "❌ 夏普比率较低，建议调整策略参数\n"
        
        max_dd = result.get('max_drawdown', 0)
        if abs(max_dd) < 0.15:
            report += "✅ 最大回撤可控\n"
        elif abs(max_dd) < 0.30:
            report += "⚠️ 最大回撤偏高，注意风险控制\n"
        else:
            report += "❌ 最大回撤过大，需要加强止损机制\n"
        
        return report


# 导出
__all__ = ['StrategyBacktesterAgent']