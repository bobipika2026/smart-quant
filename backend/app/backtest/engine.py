"""
专业量化回测框架

核心模块：
1. ICTester - IC检验器
2. GroupTester - 分组回测器
3. BacktestEngine - 回测引擎
4. PerformanceAnalyzer - 绩效分析器
5. RiskManager - 风险管理器
"""
import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import sqlite3


# 尝试导入scipy，如果不存在则使用备用实现
try:
    from scipy import stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


# ==================== 数据类 ====================

@dataclass
class BacktestConfig:
    """回测配置"""
    initial_capital: float = 1e6
    commission_rate: float = 0.0003  # 万三
    slippage_rate: float = 0.001     # 千一
    min_trade_value: float = 10000   # 最小交易金额
    max_position_weight: float = 0.1  # 单只股票最大权重
    max_turnover: float = 1.0        # 最大换手率
    risk_free_rate: float = 0.03     # 无风险利率


@dataclass
class ICTestResult:
    """IC检验结果"""
    ic_mean: float
    ic_std: float
    ir: float
    ic_positive_ratio: float
    ic_significant_ratio: float
    t_stat: float
    ic_series: pd.DataFrame = None


@dataclass
class GroupTestResult:
    """分组回测结果"""
    group_returns: Dict[str, float]
    long_short: float
    monotonic: bool
    spread: float
    group_series: pd.DataFrame = None


@dataclass
class PerformanceMetrics:
    """绩效指标"""
    # 收益指标
    total_return: float
    annual_return: float
    monthly_return: float
    
    # 风险指标
    volatility: float
    max_drawdown: float
    var_95: float
    
    # 风险调整指标
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    
    # 交易指标
    total_trades: int
    win_rate: float
    profit_loss_ratio: float


# ==================== IC检验器 ====================

class ICTester:
    """IC检验器"""
    
    def __init__(self):
        pass
    
    def calc_single_ic(self, factor_values: pd.Series, 
                       forward_returns: pd.Series,
                       method: str = 'spearman') -> Tuple[float, float]:
        """
        计算单期IC
        
        Args:
            factor_values: 因子值
            forward_returns: 未来收益率
            method: 'spearman' 或 'pearson'
        
        Returns:
            (ic值, p值)
        """
        # 对齐数据
        aligned = pd.DataFrame({
            'factor': factor_values,
            'return': forward_returns
        }).dropna()
        
        if len(aligned) < 10:
            return 0.0, 1.0
        
        if method == 'spearman':
            if HAS_SCIPY:
                ic, pvalue = stats.spearmanr(aligned['factor'], aligned['return'])
            else:
                # 手动计算spearman相关系数
                ic = self._spearman_corr(aligned['factor'], aligned['return'])
                pvalue = 0.05  # 简化处理
        else:
            if HAS_SCIPY:
                ic, pvalue = stats.pearsonr(aligned['factor'], aligned['return'])
            else:
                ic = aligned['factor'].corr(aligned['return'])
                pvalue = 0.05  # 简化处理
        
        return ic if not np.isnan(ic) else 0.0, pvalue if not np.isnan(pvalue) else 1.0
    
    def _spearman_corr(self, x: pd.Series, y: pd.Series) -> float:
        """手动计算Spearman相关系数"""
        # 计算秩
        x_rank = x.rank()
        y_rank = y.rank()
        # Pearson相关系数
        return x_rank.corr(y_rank)
    
    def calc_ic_series(self, factor_data: Dict[str, pd.Series],
                       return_data: Dict[str, pd.Series]) -> pd.DataFrame:
        """
        计算IC时间序列
        
        Args:
            factor_data: {date: factor_values}
            return_data: {date: forward_returns}
        """
        results = []
        
        for date in sorted(factor_data.keys()):
            if date not in return_data:
                continue
            
            ic, pvalue = self.calc_single_ic(factor_data[date], return_data[date])
            
            results.append({
                'date': date,
                'ic': ic,
                'pvalue': pvalue
            })
        
        return pd.DataFrame(results)
    
    def calc_ic_stats(self, ic_series: pd.DataFrame) -> ICTestResult:
        """计算IC统计指标"""
        ic_values = ic_series['ic']
        
        return ICTestResult(
            ic_mean=ic_values.mean(),
            ic_std=ic_values.std(),
            ir=ic_values.mean() / ic_values.std() if ic_values.std() > 0 else 0,
            ic_positive_ratio=(ic_values > 0).mean(),
            ic_significant_ratio=(ic_series['pvalue'] < 0.05).mean(),
            t_stat=ic_values.mean() / (ic_values.std() / len(ic_values) ** 0.5),
            ic_series=ic_series
        )
    
    def test_factor(self, factor_data: Dict, return_data: Dict) -> Dict:
        """完整的因子IC检验"""
        ic_series = self.calc_ic_series(factor_data, return_data)
        stats = self.calc_ic_stats(ic_series)
        
        return {
            'ic_mean': stats.ic_mean,
            'ic_std': stats.ic_std,
            'ir': stats.ir,
            'ic_positive_ratio': stats.ic_positive_ratio,
            'ic_significant_ratio': stats.ic_significant_ratio,
            't_stat': stats.t_stat,
            'ic_series': ic_series.to_dict('records')
        }


# ==================== 分组回测器 ====================

class GroupTester:
    """分组回测器"""
    
    def __init__(self, n_groups: int = 10):
        self.n_groups = n_groups
    
    def run_single_group_test(self, factor_values: pd.Series,
                               forward_returns: pd.Series) -> GroupTestResult:
        """
        单期分组回测
        
        Args:
            factor_values: 因子值
            forward_returns: 未来收益率
        """
        # 构建DataFrame
        df = pd.DataFrame({
            'factor': factor_values,
            'return': forward_returns
        }).dropna()
        
        if len(df) < self.n_groups * 3:  # 每组至少3只股票
            return GroupTestResult(
                group_returns={},
                long_short=0,
                monotonic=False,
                spread=0
            )
        
        # 分组
        try:
            df['group'] = pd.qcut(df['factor'], self.n_groups, labels=False, duplicates='drop')
        except:
            return GroupTestResult(
                group_returns={},
                long_short=0,
                monotonic=False,
                spread=0
            )
        
        # 计算各组收益
        group_returns = {}
        for g in sorted(df['group'].unique()):
            mask = (df['group'] == g)
            group_returns[f'G{int(g)+1}'] = df.loc[mask, 'return'].mean()
        
        # 多空收益
        g_max = f'G{max(df["group"]) + 1}'
        long_short = group_returns.get(g_max, 0) - group_returns.get('G1', 0)
        
        # 单调性检验
        returns_list = [group_returns.get(f'G{i+1}', 0) for i in range(len(group_returns))]
        monotonic = self._check_monotonic(returns_list)
        
        # spread
        spread = group_returns.get(g_max, 1) / group_returns.get('G1', 1) - 1 if group_returns.get('G1', 0) != 0 else 0
        
        return GroupTestResult(
            group_returns=group_returns,
            long_short=long_short,
            monotonic=monotonic,
            spread=spread
        )
    
    def _check_monotonic(self, values: List[float]) -> bool:
        """检查单调性"""
        if len(values) < 2:
            return False
        
        # 检查是否单调递增或递减
        increasing = all(values[i] <= values[i+1] for i in range(len(values)-1))
        decreasing = all(values[i] >= values[i+1] for i in range(len(values)-1))
        
        return increasing or decreasing
    
    def run_group_series(self, factor_data: Dict, return_data: Dict) -> pd.DataFrame:
        """分组回测时间序列"""
        results = []
        
        for date in sorted(factor_data.keys()):
            if date not in return_data:
                continue
            
            result = self.run_single_group_test(factor_data[date], return_data[date])
            
            row = {
                'date': date,
                'long_short': result.long_short,
                'monotonic': result.monotonic,
                **{f'return_{k}': v for k, v in result.group_returns.items()}
            }
            results.append(row)
        
        return pd.DataFrame(results)


# ==================== 绩效分析器 ====================

class PerformanceAnalyzer:
    """绩效分析器"""
    
    def __init__(self, risk_free_rate: float = 0.03):
        self.risk_free_rate = risk_free_rate
    
    def analyze(self, returns: pd.Series, 
                benchmark_returns: pd.Series = None,
                trades: List[Dict] = None) -> Dict:
        """
        完整绩效分析
        
        Args:
            returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
            trades: 交易记录
        """
        return {
            'return_metrics': self._calc_return_metrics(returns),
            'risk_metrics': self._calc_risk_metrics(returns),
            'risk_adjusted_metrics': self._calc_risk_adjusted(returns, benchmark_returns),
            'drawdown_analysis': self._analyze_drawdowns(returns),
            'trade_metrics': self._calc_trade_metrics(trades) if trades else {},
            'monthly_analysis': self._monthly_analysis(returns)
        }
    
    def _calc_return_metrics(self, returns: pd.Series) -> Dict:
        """收益指标"""
        return {
            'total_return': float((1 + returns).prod() - 1),
            'annual_return': float((1 + returns).prod() ** (252 / max(len(returns), 1)) - 1),
            'monthly_return': float(returns.mean() * 21),
            'daily_return_mean': float(returns.mean()),
            'positive_day_ratio': float((returns > 0).mean()),
            'best_day': float(returns.max()),
            'worst_day': float(returns.min()),
        }
    
    def _calc_risk_metrics(self, returns: pd.Series) -> Dict:
        """风险指标"""
        return {
            'volatility': float(returns.std() * np.sqrt(252)),
            'downside_deviation': float(returns[returns < 0].std() * np.sqrt(252)),
            'max_drawdown': float(self._calc_max_drawdown(returns)),
            'var_95': float(returns.quantile(0.05)),
            'cvar_95': float(returns[returns <= returns.quantile(0.05)].mean()),
            'skewness': float(self._calc_skewness(returns.dropna())),
            'kurtosis': float(self._calc_kurtosis(returns.dropna())),
        }
    
    def _calc_risk_adjusted(self, returns: pd.Series, 
                            benchmark_returns: pd.Series = None) -> Dict:
        """风险调整指标"""
        rf_daily = self.risk_free_rate / 252
        excess_returns = returns - rf_daily
        
        metrics = {
            'sharpe_ratio': float(excess_returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0,
            'sortino_ratio': float(excess_returns.mean() / returns[returns < 0].std() * np.sqrt(252)) if returns[returns < 0].std() > 0 else 0,
            'calmar_ratio': float(self._calc_calmar_ratio(returns)),
        }
        
        if benchmark_returns is not None:
            excess = returns - benchmark_returns
            metrics['information_ratio'] = float(excess.mean() / excess.std() * np.sqrt(252)) if excess.std() > 0 else 0
            metrics['tracking_error'] = float(excess.std() * np.sqrt(252))
            metrics['beta'] = float(returns.cov(benchmark_returns) / benchmark_returns.var()) if benchmark_returns.var() > 0 else 0
            metrics['alpha'] = float((1 + returns.mean()) ** 252 - 1 - metrics['beta'] * ((1 + benchmark_returns.mean()) ** 252 - 1))
        
        return metrics
    
    def _calc_max_drawdown(self, returns: pd.Series) -> float:
        """计算最大回撤"""
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.cummax()
        drawdowns = (cum_returns - running_max) / running_max
        return drawdowns.min()
    
    def _calc_calmar_ratio(self, returns: pd.Series) -> float:
        """计算卡玛比率"""
        annual_return = (1 + returns).prod() ** (252 / max(len(returns), 1)) - 1
        max_dd = abs(self._calc_max_drawdown(returns))
        return annual_return / max_dd if max_dd > 0 else 0
    
    def _calc_skewness(self, returns: pd.Series) -> float:
        """计算偏度"""
        if HAS_SCIPY:
            return stats.skew(returns)
        else:
            # 手动计算
            n = len(returns)
            mean = returns.mean()
            std = returns.std()
            if std == 0:
                return 0
            return ((returns - mean) ** 3).mean() / (std ** 3)
    
    def _calc_kurtosis(self, returns: pd.Series) -> float:
        """计算峰度（超额峰度）"""
        if HAS_SCIPY:
            return stats.kurtosis(returns)
        else:
            # 手动计算
            n = len(returns)
            mean = returns.mean()
            std = returns.std()
            if std == 0:
                return 0
            return ((returns - mean) ** 4).mean() / (std ** 4) - 3
    
    def _analyze_drawdowns(self, returns: pd.Series) -> Dict:
        """回撤分析"""
        cum_returns = (1 + returns).cumprod()
        running_max = cum_returns.cummax()
        drawdowns = (cum_returns - running_max) / running_max
        
        return {
            'max_drawdown': float(drawdowns.min()),
            'max_drawdown_start': str(drawdowns.idxmin()) if len(drawdowns) > 0 else None,
            'avg_drawdown': float(drawdowns[drawdowns < 0].mean()) if (drawdowns < 0).any() else 0,
            'drawdown_days': int((drawdowns < -0.05).sum()),  # 回撤超过5%的天数
        }
    
    def _calc_trade_metrics(self, trades: List[Dict]) -> Dict:
        """交易指标"""
        if not trades:
            return {}
        
        profits = [t.get('profit', 0) for t in trades]
        winning = [p for p in profits if p > 0]
        losing = [p for p in profits if p < 0]
        
        return {
            'total_trades': len(trades),
            'winning_trades': len(winning),
            'losing_trades': len(losing),
            'win_rate': len(winning) / len(trades) if trades else 0,
            'avg_profit': np.mean(profits) if profits else 0,
            'avg_winning': np.mean(winning) if winning else 0,
            'avg_losing': np.mean(losing) if losing else 0,
            'profit_loss_ratio': abs(np.mean(winning) / np.mean(losing)) if losing and np.mean(losing) != 0 else 0,
        }
    
    def _monthly_analysis(self, returns: pd.Series) -> Dict:
        """月度分析"""
        if len(returns) == 0:
            return {}
        
        # 按月聚合
        monthly_returns = returns.resample('M').apply(lambda x: (1 + x).prod() - 1)
        
        return {
            'monthly_returns': monthly_returns.to_dict(),
            'best_month': float(monthly_returns.max()),
            'worst_month': float(monthly_returns.min()),
            'positive_month_ratio': float((monthly_returns > 0).mean()),
        }


# ==================== 回测引擎 ====================

class BacktestEngine:
    """
    专业回测引擎
    
    支持：
    1. 因子回测 - IC检验、分组回测
    2. 策略回测 - 信号驱动、交易模拟
    """
    
    def __init__(self, config: BacktestConfig = None):
        self.config = config or BacktestConfig()
        self.ic_tester = ICTester()
        self.group_tester = GroupTester()
        self.performance_analyzer = PerformanceAnalyzer(config.risk_free_rate if config else 0.03)
        
        # 数据路径
        self.db_path = "smart_quant.db"
        self.day_cache_dir = "data_cache/day"
        self.financial_dir = "data_cache/financial"
    
    # ==================== 因子回测 ====================
    
    def backtest_factor(self, factor_values: pd.DataFrame,
                        price_data: pd.DataFrame,
                        horizon: int = 20,
                        n_groups: int = 10) -> Dict:
        """
        因子回测
        
        Args:
            factor_values: 因子值 (date x stock)
            price_data: 价格数据 (date x stock)
            horizon: 预测周期（天）
            n_groups: 分组数
        """
        # 计算未来收益率
        forward_returns = self._calc_forward_returns(price_data, horizon)
        
        # IC检验
        factor_data = {date: factor_values.loc[date] for date in factor_values.index}
        return_data = {date: forward_returns.loc[date] for date in forward_returns.index}
        
        ic_result = self.ic_tester.test_factor(factor_data, return_data)
        
        # 分组回测
        self.group_tester.n_groups = n_groups
        group_series = self.group_tester.run_group_series(factor_data, return_data)
        
        # 分组绩效
        group_performance = {}
        for g in range(n_groups):
            col = f'return_G{g+1}'
            if col in group_series.columns:
                returns = group_series[col].dropna()
                if len(returns) > 0:
                    group_performance[f'G{g+1}'] = {
                        'total_return': float((1 + returns).prod() - 1),
                        'annual_return': float((1 + returns.mean()) ** 252 - 1),
                        'sharpe': float(returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0,
                    }
        
        return {
            'ic_analysis': ic_result,
            'group_analysis': {
                'long_short_return': float((1 + group_series['long_short'].dropna()).prod() - 1),
                'long_short_sharpe': float(group_series['long_short'].mean() / group_series['long_short'].std() * np.sqrt(252)) if group_series['long_short'].std() > 0 else 0,
                'monotonic_ratio': float(group_series['monotonic'].mean()),
                'group_performance': group_performance,
            },
            'summary': {
                'factor_effective': ic_result['ir'] > 0.3 and ic_result['ic_mean'] > 0.02,
                'recommendation': self._generate_recommendation(ic_result, group_series)
            }
        }
    
    def _calc_forward_returns(self, price_data: pd.DataFrame, horizon: int) -> pd.DataFrame:
        """计算未来收益率"""
        returns = price_data.pct_change(horizon).shift(-horizon)
        return returns
    
    def _generate_recommendation(self, ic_result: Dict, group_series: pd.DataFrame) -> str:
        """生成因子评价建议"""
        if ic_result['ir'] > 0.5 and ic_result['ic_mean'] > 0.03:
            return "因子效果优秀，建议纳入因子库"
        elif ic_result['ir'] > 0.3 and ic_result['ic_mean'] > 0.02:
            return "因子效果良好，可考虑纳入"
        elif ic_result['ir'] > 0.2:
            return "因子效果一般，需进一步优化"
        else:
            return "因子效果较弱，不建议使用"
    
    # ==================== 策略回测 ====================
    
    def backtest_strategy(self, signals: pd.DataFrame,
                          price_data: pd.DataFrame,
                          initial_capital: float = None) -> Dict:
        """
        策略回测
        
        Args:
            signals: 信号 (date x stock), 1=买入, -1=卖出, 0=持有
            price_data: 价格数据
            initial_capital: 初始资金
        """
        capital = initial_capital or self.config.initial_capital
        
        # 模拟交易
        portfolio_values = []
        positions = {}
        cash = capital
        trades = []
        
        for date in signals.index:
            if date not in price_data.index:
                continue
            
            prices = price_data.loc[date]
            daily_signals = signals.loc[date]
            
            # 执行交易
            for stock in signals.columns:
                if stock not in prices or pd.isna(prices[stock]):
                    continue
                
                signal = daily_signals.get(stock, 0)
                current_pos = positions.get(stock, 0)
                
                if signal == 1 and current_pos == 0:  # 买入
                    shares = (cash * self.config.max_position_weight) / prices[stock]
                    cost = shares * prices[stock] * (1 + self.config.commission_rate)
                    if cost > self.config.min_trade_value:
                        positions[stock] = shares
                        cash -= cost
                        trades.append({
                            'date': date,
                            'stock': stock,
                            'action': 'buy',
                            'shares': shares,
                            'price': prices[stock],
                            'cost': cost
                        })
                
                elif signal == -1 and current_pos > 0:  # 卖出
                    revenue = current_pos * prices[stock] * (1 - self.config.commission_rate)
                    cash += revenue
                    trades.append({
                        'date': date,
                        'stock': stock,
                        'action': 'sell',
                        'shares': current_pos,
                        'price': prices[stock],
                        'revenue': revenue
                    })
                    del positions[stock]
            
            # 计算组合价值
            position_value = sum(positions.get(s, 0) * prices.get(s, 0) for s in positions if s in prices)
            total_value = cash + position_value
            portfolio_values.append({
                'date': date,
                'value': total_value,
                'cash': cash,
                'position_value': position_value
            })
        
        # 计算收益
        portfolio_df = pd.DataFrame(portfolio_values).set_index('date')
        returns = portfolio_df['value'].pct_change()
        
        # 绩效分析
        performance = self.performance_analyzer.analyze(returns, trades=trades)
        
        return {
            'performance': performance,
            'portfolio_history': portfolio_df.to_dict('records'),
            'trade_count': len(trades),
            'final_value': float(portfolio_df['value'].iloc[-1]) if len(portfolio_df) > 0 else capital,
        }


# ==================== 单例 ====================

_backtest_engine = None

def get_backtest_engine(config: BacktestConfig = None) -> BacktestEngine:
    global _backtest_engine
    if _backtest_engine is None:
        _backtest_engine = BacktestEngine(config)
    return _backtest_engine