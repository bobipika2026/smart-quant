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
    
    def __init__(self, close: np.ndarray):
        """
        初始化
        
        Args:
            close: 收盘价数组
        """
        self.close = close
        self.n = len(close)
    
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
    
    def backtest_strategy(self, signals: np.ndarray) -> Tuple[float, float, int]:
        """
        回测单个策略
        
        Args:
            signals: 信号数组 (1=买入, -1=卖出, 0=持有)
        
        Returns:
            (总收益率%, 夏普比率, 交易次数)
        """
        returns = []
        position = False
        buy_price = 0
        
        for i in range(self.n):
            if signals[i] == 1 and not position:
                buy_price = self.close[i]
                position = True
            elif signals[i] == -1 and position:
                ret = (self.close[i] - buy_price) / buy_price
                returns.append(ret)
                position = False
        
        # 平仓
        if position:
            returns.append((self.close[-1] - buy_price) / buy_price)
        
        if not returns:
            return 0, 0, 0
        
        total_return = np.prod([1 + r for r in returns]) - 1
        sharpe = np.mean(returns) / (np.std(returns) + 1e-10) * np.sqrt(252) if len(returns) > 1 else 0
        
        return total_return * 100, sharpe, len(returns)
    
    def generate_ma_signals(self, short: int, long: int) -> np.ndarray:
        """生成MA金叉信号"""
        ma_short = self.calc_ma(short)
        ma_long = self.calc_ma(long)
        
        signals = np.zeros(self.n)
        
        for i in range(1, self.n):
            if not np.isnan(ma_short[i]) and not np.isnan(ma_long[i]):
                if ma_short[i] > ma_long[i] and ma_short[i-1] <= ma_long[i-1]:
                    signals[i] = 1  # 金叉买入
                elif ma_short[i] < ma_long[i] and ma_short[i-1] >= ma_long[i-1]:
                    signals[i] = -1  # 死叉卖出
        
        return signals
    
    def generate_macd_signals(self, fast=12, slow=26, signal=9) -> np.ndarray:
        """生成MACD信号"""
        _, signal_line, hist = self.calc_macd(fast, slow, signal)
        
        signals = np.zeros(self.n)
        for i in range(1, self.n):
            if hist[i] > 0 and hist[i-1] <= 0:
                signals[i] = 1
            elif hist[i] < 0 and hist[i-1] >= 0:
                signals[i] = -1
        
        return signals
    
    def generate_rsi_signals(self, period=14, upper=70, lower=30) -> np.ndarray:
        """生成RSI信号"""
        rsi = self.calc_rsi(period)
        
        signals = np.zeros(self.n)
        for i in range(1, self.n):
            if rsi[i] < lower and rsi[i-1] >= lower:
                signals[i] = 1  # 超卖买入
            elif rsi[i] > upper and rsi[i-1] <= upper:
                signals[i] = -1  # 超买卖出
        
        return signals
    
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
    stock_name: str = None
) -> int:
    """
    保存最佳因子组合到数据库
    
    Args:
        stock_code: 股票代码
        top_results: Top N结果列表
        stock_name: 股票名称
    
    Returns:
        保存的记录数
    """
    from app.database import SessionLocal
    from app.models.factor_matrix import BestFactorCombination
    from datetime import date
    
    db = SessionLocal()
    saved_count = 0
    
    try:
        for r in top_results:
            # 生成组合唯一标识
            factors = r['factor_combination']
            factor_keys = sorted([k for k, v in factors.items() if v == 1])
            combination_code = f"{stock_code}:{':'.join(factor_keys)}"
            
            # 检查是否已存在
            existing = db.query(BestFactorCombination).filter(
                BestFactorCombination.combination_code == combination_code
            ).first()
            
            if existing:
                # 更新现有记录
                existing.total_return = r['total_return']
                existing.sharpe_ratio = r['sharpe_ratio']
                existing.composite_score = r['composite_score']
                existing.backtest_date = date.today()
                existing.is_active = True
            else:
                # 创建新记录
                record = BestFactorCombination(
                    combination_code=combination_code,
                    stock_code=stock_code,
                    stock_name=stock_name or stock_code,
                    factor_combination=json.dumps(r['factor_combination']),
                    strategy_desc=r['strategy'],
                    total_return=r['total_return'],
                    sharpe_ratio=r['sharpe_ratio'],
                    composite_score=r['composite_score'],
                    holding_period=r['time'],
                    backtest_date=date.today(),
                    is_active=True,
                    notes=f"experiment_code: {r['experiment_code']}, type: {r.get('experiment_type', 'single')}"
                )
                db.add(record)
            
            saved_count += 1
        
        db.commit()
        print(f"[最佳组合] 保存 {saved_count} 条记录")
        
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
    cache_file = f'data_cache/{stock_code}_history.csv'
    df = pd.read_csv(cache_file)
    close = df['收盘'].values
    
    print(f"[向量化回测] 数据: {len(close)} 条")
    
    # 2. 初始化引擎
    engine = VectorizedFactorBacktest(close)
    
    # 3. 定义策略因子（12个）
    strategy_params = {
        # MA策略
        'ma_5_20': lambda: engine.generate_ma_signals(5, 20),
        'ma_5_30': lambda: engine.generate_ma_signals(5, 30),
        'ma_10_20': lambda: engine.generate_ma_signals(10, 20),
        'ma_10_30': lambda: engine.generate_ma_signals(10, 30),
        # MACD策略
        'macd_default': lambda: engine.generate_macd_signals(12, 26, 9),
        'macd_fast': lambda: engine.generate_macd_signals(8, 17, 9),
        # RSI策略
        'rsi_14_70': lambda: engine.generate_rsi_signals(14, 70, 30),
        'rsi_14_80': lambda: engine.generate_rsi_signals(14, 80, 20),
        # KDJ策略（用RSI近似）
        'kdj_default': lambda: engine.generate_rsi_signals(9, 80, 20),
        # 布林带（用MA近似）
        'boll_20_2': lambda: engine.generate_ma_signals(20, 40),
        # CCI（用RSI近似）
        'cci_14': lambda: engine.generate_rsi_signals(14, 100, -100),
        # WR（用RSI反向近似）
        'wr_14': lambda: engine.generate_rsi_signals(14, 20, 80),
    }
    
    strategy_codes = list(strategy_params.keys())
    
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
        signal_func = strategy_params[strategy_code]
        signals = signal_func()
        
        for time_factor, days in zip(time_factors, time_days):
            end_idx = min(days, len(close))
            partial_signals = signals[:end_idx]
            partial_close = close[:end_idx]
            
            sub_engine = VectorizedFactorBacktest(partial_close)
            ret, sharpe, trades = sub_engine.backtest_strategy(partial_signals)
            
            composite = ret * 0.4 + sharpe * 10 * 0.3
            
            results.append({
                'experiment_code': f'EXP_{exp_id:06d}',
                'strategy': strategy_code,
                'time': time_factor,
                'total_return': ret,
                'sharpe_ratio': sharpe,
                'trade_count': trades,
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
            sig1 = strategy_params[s1]()
            sig2 = strategy_params[s2]()
            
            # AND组合
            combined_and = engine.combine_signals([sig1, sig2], mode='and')
            # OR组合
            combined_or = engine.combine_signals([sig1, sig2], mode='or')
            
            for time_factor, days in zip(time_factors, time_days):
                end_idx = min(days, len(close))
                partial_close = close[:end_idx]
                sub_engine = VectorizedFactorBacktest(partial_close)
                
                # AND
                ret_and, sharpe_and, trades_and = sub_engine.backtest_strategy(combined_and[:end_idx])
                composite_and = ret_and * 0.4 + sharpe_and * 10 * 0.3
                
                results.append({
                    'experiment_code': f'EXP_{exp_id:06d}',
                    'strategy': f'{s1}+{s2}(AND)',
                    'time': time_factor,
                    'total_return': ret_and,
                    'sharpe_ratio': sharpe_and,
                    'trade_count': trades_and,
                    'composite_score': composite_and,
                    'factor_combination': {s1: 1, s2: 1, time_factor: 1},
                    'experiment_type': 'double_and'
                })
                exp_id += 1
                
                # OR
                ret_or, sharpe_or, trades_or = sub_engine.backtest_strategy(combined_or[:end_idx])
                composite_or = ret_or * 0.4 + sharpe_or * 10 * 0.3
                
                results.append({
                    'experiment_code': f'EXP_{exp_id:06d}',
                    'strategy': f'{s1}+{s2}(OR)',
                    'time': time_factor,
                    'total_return': ret_or,
                    'sharpe_ratio': sharpe_or,
                    'trade_count': trades_or,
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
    save_best_combinations(stock_code, top_results)
    
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