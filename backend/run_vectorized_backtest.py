"""
向量化因子矩阵回测 - 高性能版
速度：15000+ 实验/秒
"""
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple
import itertools
import json


class VectorizedFactorBacktest:
    """向量化因子回测引擎"""
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化
        
        Args:
            df: 包含日期和收盘价的DataFrame
        """
        self.df = df
        self.close = df['收盘'].values
        self.n = len(df)
    
    def calc_ma(self, window: int) -> np.ndarray:
        """计算均线"""
        if window > self.n:
            return np.full(self.n, np.nan)
        
        ma = np.convolve(self.close, np.ones(window)/window, mode='valid')
        result = np.full(self.n, np.nan)
        result[window-1:] = ma
        return result
    
    def calc_macd(self, fast=12, slow=26, signal=9):
        """计算MACD"""
        ema_fast = pd.Series(self.close).ewm(span=fast, adjust=False).mean().values
        ema_slow = pd.Series(self.close).ewm(span=slow, adjust=False).mean().values
        macd_line = ema_fast - ema_slow
        signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
        hist = macd_line - signal_line
        return macd_line, signal_line, hist
    
    def calc_rsi(self, period=14):
        """计算RSI"""
        delta = np.diff(self.close)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)
        
        avg_gain = pd.Series(gain).rolling(period).mean().values
        avg_loss = pd.Series(loss).rolling(period).mean().values
        
        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))
        return np.concatenate([[50], rsi])  # 补齐长度
    
    def backtest_strategy(self, signals: np.ndarray) -> Dict:
        """
        回测单个策略 - 使用BacktestEngine保证一致性
        
        Args:
            signals: 信号数组 (1=买入, -1=卖出, 0=持有)
        
        Returns:
            回测结果字典
        """
        from app.services.backtest import BacktestEngine
        
        # 构建DataFrame（使用真实数据）
        df = self.df.copy()
        df['signal'] = signals
        
        # 使用BacktestEngine计算
        engine = BacktestEngine()
        result = engine.run_backtest(df, signal_col='signal')
        
        return {
            'total_return': result.get('total_return', 0),
            'annual_return': result.get('annual_return'),
            'sharpe_ratio': result.get('sharpe_ratio', 0),
            'max_drawdown': result.get('max_drawdown'),
            'win_rate': result.get('win_rate'),
            'profit_loss_ratio': result.get('profit_loss_ratio'),
            'trade_count': result.get('trade_count', 0)
        }
    
    def generate_ma_signals(self, short: int, long: int) -> np.ndarray:
        """生成MA金叉信号 - 使用回测引擎的策略"""
        from app.services.strategy import MAStrategy
        
        df = self.df.copy()
        strategy = MAStrategy(short_period=short, long_period=long)
        df_signal = strategy.generate_signals(df)
        return df_signal['signal'].values
    
    def generate_macd_signals(self, fast=12, slow=26, signal=9) -> np.ndarray:
        """生成MACD信号 - 使用回测引擎的策略"""
        from app.services.strategy import MACDStrategy
        
        df = self.df.copy()
        strategy = MACDStrategy(fast=fast, slow=slow, signal=signal)
        df_signal = strategy.generate_signals(df)
        return df_signal['signal'].values
    
    def generate_rsi_signals(self, period=14, upper=70, lower=30) -> np.ndarray:
        """生成RSI信号 - 使用回测引擎的策略"""
        from app.services.strategy import RSIStrategy
        
        df = self.df.copy()
        strategy = RSIStrategy(period=period, oversold=lower, overbought=upper)
        df_signal = strategy.generate_signals(df)
        return df_signal['signal'].values
    
    def combine_signals(self, signal_list: List[np.ndarray], mode='and') -> np.ndarray:
        """
        组合多个策略信号
        
        Args:
            signal_list: 信号列表
            mode: 'and' | 'or' | 'vote'
        
        Returns:
            组合信号
        """
        if len(signal_list) == 1:
            return signal_list[0]
        
        signals = np.stack(signal_list, axis=0)
        combined = np.zeros(self.n)
        
        if mode == 'and':
            # 所有策略都买入才买入
            buy = (signals == 1).all(axis=0)
            sell = (signals == -1).all(axis=0)
        elif mode == 'or':
            # 任一策略买入就买入
            buy = (signals == 1).any(axis=0)
            sell = (signals == -1).any(axis=0)
        else:  # vote
            # 多数投票
            vote = signals.sum(axis=0)
            buy = vote >= len(signal_list) / 2
            sell = vote <= -len(signal_list) / 2
        
        combined[buy] = 1
        combined[sell] = -1
        
        return combined


def save_best_combinations(
    stock_code: str,
    top_results: List[Dict],
    stock_name: str = None,
    max_per_stock: int = 3,
    benchmark_return: float = None
) -> int:
    """
    保存最佳因子组合到数据库（每只股票只保存 Top N）
    
    Args:
        stock_code: 股票代码
        top_results: Top N结果列表
        stock_name: 股票名称
        max_per_stock: 每只股票最多保存几条
        benchmark_return: 基准收益（买入持有）
    
    Returns:
        保存的记录数
    """
    from app.database import SessionLocal
    from app.models.factor_matrix import BestFactorCombination
    from datetime import date
    
    db = SessionLocal()
    saved_count = 0
    
    try:
        # 先删除该股票的所有旧记录（避免combination_code唯一约束冲突）
        db.query(BestFactorCombination).filter(
            BestFactorCombination.stock_code == stock_code
        ).delete()
        db.commit()  # 立即提交删除
        
        # 只保存前 max_per_stock 个
        for rank, r in enumerate(top_results[:max_per_stock], 1):
            # 生成组合唯一标识
            factors = r['factor_combination']
            factor_keys = sorted([k for k, v in factors.items() if v == 1])
            combination_code = f"{stock_code}:{':'.join(factor_keys)}"
            
            # 创建新记录
            record = BestFactorCombination(
                combination_code=combination_code,
                stock_code=stock_code,
                stock_name=stock_name or stock_code,
                factor_combination=json.dumps(r['factor_combination']),
                strategy_desc=r['strategy'],
                rank_in_stock=rank,
                total_return=r['total_return'],
                annual_return=r.get('annual_return'),
                benchmark_return=benchmark_return,
                sharpe_ratio=r['sharpe_ratio'],
                max_drawdown=r.get('max_drawdown'),
                win_rate=r.get('win_rate'),
                profit_loss_ratio=r.get('profit_loss_ratio'),
                trade_count=r['trade_count'],
                composite_score=r['composite_score'],
                holding_period=r['time'],
                backtest_date=date.today(),
                is_active=True,
                notes=f"experiment_code: {r['experiment_code']}, type: {r.get('experiment_type', 'single')}"
            )
            db.add(record)
            saved_count += 1
        
        db.commit()
        print(f"[最佳组合] 保存 {saved_count} 条记录 (每股票最多{max_per_stock}条)")
        
    except Exception as e:
        db.rollback()
        print(f"[最佳组合] 保存失败: {e}")
    finally:
        db.close()
    
    return saved_count


def run_vectorized_factor_matrix(
    stock_code: str,
    top_n: int = 10,
    include_all_combinations: bool = True
) -> Dict:
    """
    运行向量化因子矩阵回测
    
    Args:
        stock_code: 股票代码
        top_n: 返回Top N结果
        include_all_combinations: 是否包含所有因子组合
    
    Returns:
        回测结果
    """
    from app.database import SessionLocal
    from app.models.factor_matrix import FactorExperiment
    
    print(f"[向量化回测] 股票: {stock_code}")
    
    # 1. 加载本地数据
    cache_file = f'data_cache/day/{stock_code}_day.csv'
    df = pd.read_csv(cache_file)
    # 确保日期格式正确
    df['日期'] = pd.to_datetime(df['日期'].astype(str))
    df = df.sort_values('日期').reset_index(drop=True)
    
    print(f"[向量化回测] 数据: {len(df)} 条")
    
    # 计算基准收益（买入持有）
    benchmark_return = round((df['收盘'].iloc[-1] / df['收盘'].iloc[0] - 1) * 100, 2)
    print(f"[向量化回测] 基准收益: {benchmark_return}%")
    
    # 2. 初始化引擎（用于信号生成）
    engine = VectorizedFactorBacktest(df)
    
    # 3. 定义策略生成函数（不绑定特定engine）
    def generate_signals_for_strategy(engine_instance, strategy_code):
        """根据策略代码生成信号 - 使用策略类保证一致性"""
        from app.services.strategy import get_strategy
        
        # 策略ID映射
        strategy_map = {
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
        
        if strategy_code not in strategy_map:
            return np.zeros(engine_instance.n)
        
        strategy_id, params = strategy_map[strategy_code]
        
        # 使用策略类生成信号
        strategy = get_strategy(strategy_id)
        strategy.params = params
        df_signal = strategy.generate_signals(engine_instance.df.copy())
        
        return df_signal['signal'].values
    
    strategy_codes = ['ma_5_20', 'ma_5_30', 'ma_10_20', 'ma_10_30',
                      'macd_default', 'macd_fast',
                      'rsi_14_70', 'rsi_14_80',
                      'kdj_default', 'boll_20_2', 'cci_14', 'wr_14']
    
    # 时间因子
    time_factors = ['period_3m', 'period_6m', 'period_1y', 'period_2y']
    time_days = [63, 126, 252, 504]
    
    # 4. 生成实验
    print("[向量化回测] 生成实验...")
    
    start_time = datetime.now()
    results = []
    exp_id = 1
    
    # ===== 单策略实验 =====
    print("[向量化回测] 单策略实验...")
    for strategy_code in strategy_codes:
        for time_factor, days in zip(time_factors, time_days):
            # 从数据末尾往前取（最近N天）
            start_idx = max(0, len(df) - days)
            partial_df = df.iloc[start_idx:].copy()
            
            # 对截取后的数据生成信号
            partial_engine = VectorizedFactorBacktest(partial_df)
            signals = generate_signals_for_strategy(partial_engine, strategy_code)
            
            # 回测
            result = partial_engine.backtest_strategy(signals)
            
            composite = result['total_return'] * 0.4 + result['sharpe_ratio'] * 10 * 0.3
            
            results.append({
                'experiment_code': f'{stock_code}_{exp_id:06d}',
                'strategy': strategy_code,
                'time': time_factor,
                'total_return': result['total_return'],
                'annual_return': result.get('annual_return'),
                'sharpe_ratio': result['sharpe_ratio'],
                'max_drawdown': result.get('max_drawdown'),
                'win_rate': result.get('win_rate'),
                'profit_loss_ratio': result.get('profit_loss_ratio'),
                'trade_count': result['trade_count'],
                'composite_score': composite,
                'factor_combination': {strategy_code: 1, time_factor: 1},
                'experiment_type': 'single'
            })
            exp_id += 1
    
    # ===== 双策略组合 =====
    if include_all_combinations:
        print("[向量化回测] 双策略组合...")
        
        # 定义互斥组
        ma_strategies = ['ma_5_20', 'ma_5_30', 'ma_10_20', 'ma_10_30']
        macd_strategies = ['macd_default', 'macd_fast']
        rsi_strategies = ['rsi_14_70', 'rsi_14_80']
        
        # 生成所有有效组合（避免互斥）
        valid_pairs = []
        for i, s1 in enumerate(strategy_codes):
            for s2 in strategy_codes[i+1:]:
                # 检查互斥
                if s1 in ma_strategies and s2 in ma_strategies:
                    continue
                if s1 in macd_strategies and s2 in macd_strategies:
                    continue
                if s1 in rsi_strategies and s2 in rsi_strategies:
                    continue
                valid_pairs.append((s1, s2))
        
        print(f"  有效组合数: {len(valid_pairs)}")
        
        for s1, s2 in valid_pairs:
            for time_factor, days in zip(time_factors, time_days):
                # 从数据末尾往前取（最近N天）
                start_idx = max(0, len(df) - days)
                partial_df = df.iloc[start_idx:].copy()
                
                # 对截取后的数据生成信号
                partial_engine = VectorizedFactorBacktest(partial_df)
                sig1 = generate_signals_for_strategy(partial_engine, s1)
                sig2 = generate_signals_for_strategy(partial_engine, s2)
                
                # AND组合
                combined_and = partial_engine.combine_signals([sig1, sig2], mode='and')
                # OR组合
                combined_or = partial_engine.combine_signals([sig1, sig2], mode='or')
                
                # AND
                result_and = partial_engine.backtest_strategy(combined_and)
                composite_and = result_and['total_return'] * 0.4 + result_and['sharpe_ratio'] * 10 * 0.3
                
                results.append({
                    'experiment_code': f'{stock_code}_{exp_id:06d}',
                    'strategy': f'{s1}+{s2}(AND)',
                    'time': time_factor,
                    'total_return': result_and['total_return'],
                    'annual_return': result_and.get('annual_return'),
                    'sharpe_ratio': result_and['sharpe_ratio'],
                    'max_drawdown': result_and.get('max_drawdown'),
                    'win_rate': result_and.get('win_rate'),
                    'profit_loss_ratio': result_and.get('profit_loss_ratio'),
                    'trade_count': result_and['trade_count'],
                    'composite_score': composite_and,
                    'factor_combination': {s1: 1, s2: 1, time_factor: 1},
                    'experiment_type': 'double_and'
                })
                exp_id += 1
                
                # OR
                result_or = partial_engine.backtest_strategy(combined_or)
                composite_or = result_or['total_return'] * 0.4 + result_or['sharpe_ratio'] * 10 * 0.3
                
                results.append({
                    'experiment_code': f'{stock_code}_{exp_id:06d}',
                    'strategy': f'{s1}+{s2}(OR)',
                    'time': time_factor,
                    'total_return': result_or['total_return'],
                    'annual_return': result_or.get('annual_return'),
                    'sharpe_ratio': result_or['sharpe_ratio'],
                    'max_drawdown': result_or.get('max_drawdown'),
                    'win_rate': result_or.get('win_rate'),
                    'profit_loss_ratio': result_or.get('profit_loss_ratio'),
                    'trade_count': result_or['trade_count'],
                    'composite_score': composite_or,
                    'factor_combination': {s1: 1, s2: 1, time_factor: 1},
                    'experiment_type': 'double_or'
                })
                exp_id += 1
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print(f"[向量化回测] 完成 {len(results)} 个实验, 耗时 {elapsed:.2f}秒")
    
    # 5. 排序
    results.sort(key=lambda x: x['composite_score'], reverse=True)
    
    # 6. 保存Top N
    top_results = results[:top_n]
    
    db = SessionLocal()
    try:
        db.query(FactorExperiment).filter(FactorExperiment.stock_code == stock_code).delete()
        
        for r in top_results:
            exp_record = FactorExperiment(
                experiment_code=r['experiment_code'],
                stock_code=stock_code,
                factor_combination=json.dumps(r['factor_combination']),
                active_factor_count=len(r['factor_combination']),
                total_return=r['total_return'],
                sharpe_ratio=r['sharpe_ratio'],
                win_rate=None,
                trade_count=r['trade_count'],
                notes=f"composite_score: {r['composite_score']:.2f}, strategy: {r['strategy']}, time: {r['time']}"
            )
            db.add(exp_record)
        
        db.commit()
        print(f"[向量化回测] 保存 Top {len(top_results)} 结果到 FactorExperiment")
    except Exception as e:
        db.rollback()
        print(f"[向量化回测] 保存失败: {e}")
    finally:
        db.close()
    
    # 7. 保存到最佳因子组合表
    save_best_combinations(stock_code, top_results, benchmark_return=benchmark_return)
    
    return {
        'stock_code': stock_code,
        'total_experiments': len(results),
        'elapsed_seconds': elapsed,
        'experiments_per_second': len(results) / elapsed if elapsed > 0 else 0,
        'top_results': [
            {
                'rank': i + 1,
                'experiment_code': r['experiment_code'],
                'strategy': r['strategy'],
                'time': r['time'],
                'total_return': r['total_return'],
                'sharpe_ratio': r['sharpe_ratio'],
                'composite_score': round(r['composite_score'], 2),
                'factor_combination': r['factor_combination'],
                'experiment_type': r.get('experiment_type', 'single')
            }
            for i, r in enumerate(top_results)
        ]
    }


if __name__ == "__main__":
    import sys
    
    stock_code = sys.argv[1] if len(sys.argv) > 1 else "000001"
    
    result = run_vectorized_factor_matrix(stock_code, top_n=10)
    
    print("\n" + "=" * 60)
    print(f"TOP 10 因子组合 (股票: {stock_code})")
    print("=" * 60)
    print(f"总实验: {result['total_experiments']}")
    print(f"耗时: {result['elapsed_seconds']:.2f}秒")
    print(f"速度: {result['experiments_per_second']:.0f} 实验/秒")
    
    for r in result['top_results']:
        print(f"\n#{r['rank']}: {r['experiment_code']}")
        print(f"  策略: {r['strategy']}")
        print(f"  时间: {r['time']}")
        print(f"  综合得分: {r['composite_score']}")
        print(f"  收益: {r['total_return']:.2f}%, 夏普: {r['sharpe_ratio']:.2f}")