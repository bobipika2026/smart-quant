"""
股票级别因子权重优化

为每只股票定制最优因子权重组合
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from itertools import product
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class StockLevelOptimizer:
    """股票级别因子权重优化器"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 可用因子池
    AVAILABLE_FACTORS = ['KDJ', 'BOLL', 'MOM', 'LEV', 'ROE', 'TURN', 'EP', 'BP']
    
    # 权重候选（简化为3档）
    WEIGHT_OPTIONS = [0.0, 0.1, 0.2, 0.3]
    
    def __init__(self):
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    def get_stock_list(self, limit: int = 50) -> List[str]:
        """获取股票列表"""
        try:
            conn = sqlite3.connect(self.DB_PATH)
            cursor = conn.execute(f"""
                SELECT DISTINCT stock_code 
                FROM best_factor_combinations 
                WHERE is_active = 1
                LIMIT {limit}
            """)
            stocks = [row[0] for row in cursor.fetchall()]
            conn.close()
            return stocks
        except:
            files = [f.replace('_day.csv', '').replace('.SZ', '').replace('.SH', '') 
                    for f in os.listdir(self.DAY_CACHE_DIR) if f.endswith('_day.csv')]
            return sorted(set(files))[:limit]
    
    def get_stock_data(self, stock_code: str) -> pd.DataFrame:
        """获取股票数据"""
        possible_files = [
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SZ_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SH_day.csv"),
        ]
        
        for fp in possible_files:
            if os.path.exists(fp):
                df = pd.read_csv(fp, encoding='utf-8')
                column_map = {'日期': 'trade_date', '开盘': 'open', '最高': 'high', 
                              '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'}
                df = df.rename(columns=column_map)
                df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
                df.set_index('trade_date', inplace=True)
                return df[['open', 'high', 'low', 'close', 'volume', 'amount']]
        
        return pd.DataFrame()
    
    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算因子"""
        result = df.copy()
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ'] = rsv.ewm(alpha=1/3).mean().ewm(alpha=1/3).mean()
        
        # BOLL
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # MOM
        result['MOM'] = result['close'].pct_change(63)
        
        # LEV
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # ROE (简化)
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                       (result['close'].pct_change().rolling(252).std() + 0.0001)
        
        # TURN
        result['TURN'] = result['volume'].rolling(5).mean() / \
                        (result['volume'].rolling(60).mean() + 0.0001) - 1
        
        # EP, BP
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        return result
    
    def calculate_ic(self, df: pd.DataFrame, factor: str, forward_period: int = 20) -> float:
        """计算因子IC（与未来收益的相关性）"""
        if factor not in df.columns:
            return 0.0
        
        factor_series = df[factor]
        future_return = df['close'].pct_change(forward_period).shift(-forward_period)
        
        # 计算Spearman相关系数
        valid_data = pd.DataFrame({'factor': factor_series, 'return': future_return}).dropna()
        
        if len(valid_data) < 100:
            return 0.0
        
        try:
            from scipy.stats import spearmanr
            ic, _ = spearmanr(valid_data['factor'], valid_data['return'])
            return ic if not np.isnan(ic) else 0.0
        except:
            return 0.0
    
    def optimize_single_stock(self, stock_code: str) -> Dict:
        """优化单只股票的因子权重"""
        df = self.get_stock_data(stock_code)
        if df.empty or len(df) < 1000:
            return None
        
        df = self.calculate_factors(df)
        
        # 计算每个因子的IC
        factor_ics = {}
        for factor in self.AVAILABLE_FACTORS:
            ic = self.calculate_ic(df, factor)
            factor_ics[factor] = ic
        
        # 基于IC确定权重（IC绝对值越大，权重越高）
        total_ic = sum([abs(ic) for ic in factor_ics.values()])
        
        if total_ic > 0:
            factor_weights = {f: round(abs(ic) / total_ic, 3) for f, ic in factor_ics.items()}
        else:
            factor_weights = {f: 1.0 / len(self.AVAILABLE_FACTORS) for f in self.AVAILABLE_FACTORS}
        
        # 筛选有效因子（IC绝对值 > 0.02）
        effective_factors = {f: w for f, w in factor_weights.items() 
                           if abs(factor_ics.get(f, 0)) > 0.01}
        
        if not effective_factors:
            effective_factors = dict(list(factor_weights.items())[:4])  # 取前4个
        
        # 归一化权重
        total_weight = sum(effective_factors.values())
        if total_weight > 0:
            effective_factors = {f: round(w / total_weight, 3) for f, w in effective_factors.items()}
        
        # 回测验证
        df = self.backtest_with_weights(df, effective_factors)
        
        # 计算绩效
        if 'strategy_returns' not in df.columns:
            return None
        
        df = df.dropna()
        if len(df) < 500:
            return None
        
        total_return = (1 + df['strategy_returns']).prod() - 1
        annual_return = (1 + total_return) ** (252 / len(df)) - 1
        volatility = df['strategy_returns'].std() * np.sqrt(252)
        sharpe = annual_return / volatility if volatility > 0 else 0
        
        cum_returns = (1 + df['strategy_returns']).cumprod()
        running_max = cum_returns.cummax()
        drawdown = (cum_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        
        benchmark_return = df['close'].iloc[-1] / df['close'].iloc[0] - 1
        benchmark_annual = (1 + benchmark_return) ** (252 / len(df)) - 1
        
        return {
            'stock_code': stock_code,
            'factor_ics': {k: round(v, 4) for k, v in factor_ics.items()},
            'optimal_weights': effective_factors,
            'annual_return': round(annual_return * 100, 2),
            'sharpe_ratio': round(sharpe, 3),
            'max_drawdown': round(max_drawdown * 100, 2),
            'excess_return': round((annual_return - benchmark_annual) * 100, 2)
        }
    
    def backtest_with_weights(self, df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
        """使用指定权重回测"""
        result = df.copy()
        
        # 计算加权得分
        result['score'] = 0
        for factor, weight in weights.items():
            if factor in result.columns:
                factor_std = (result[factor] - result[factor].rolling(252).mean()) / \
                            (result[factor].rolling(252).std() + 0.0001)
                result['score'] += factor_std.fillna(0) * weight
        
        # 标准化
        result['score_z'] = (result['score'] - result['score'].rolling(252).mean()) / \
                           (result['score'].rolling(252).std() + 0.0001)
        
        # 信号
        result['signal'] = 0
        result.loc[result['score_z'] > 0.3, 'signal'] = 1
        result.loc[result['score_z'] < -0.3, 'signal'] = -1
        
        # 仓位（动态）
        vol = result['close'].pct_change().rolling(20).std() * np.sqrt(252)
        vol_adj = 0.2 / (vol + 0.0001)
        vol_adj = vol_adj.clip(0.3, 0.9)
        
        result['position'] = vol_adj * result['signal'].abs()
        
        # 收益
        result['returns'] = result['close'].pct_change()
        result['strategy_returns'] = result['position'].shift(1) * result['returns']
        
        return result
    
    def optimize_all(self, stock_limit: int = 50) -> Dict:
        """优化所有股票"""
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("股票级别因子权重优化")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"方法: 根据IC为每只股票定制因子权重")
        print(f"{'='*70}\n")
        
        results = []
        
        for i, stock in enumerate(stocks, 1):
            if i % 10 == 0:
                print(f"进度: {i}/{len(stocks)}")
                sys.stdout.flush()
            
            result = self.optimize_single_stock(stock)
            if result:
                results.append(result)
        
        if not results:
            return {'status': 'no_results'}
        
        df_results = pd.DataFrame(results)
        
        # 汇总
        print(f"\n{'='*70}")
        print("优化结果")
        print(f"{'='*70}")
        print(f"成功优化: {len(results)}/{len(stocks)}")
        print(f"\n平均年化收益: {df_results['annual_return'].mean():.2f}%")
        print(f"平均夏普比率: {df_results['sharpe_ratio'].mean():.3f}")
        print(f"平均最大回撤: {df_results['max_drawdown'].mean():.2f}%")
        print(f"跑赢基准占比: {(df_results['excess_return'] > 0).sum() / len(df_results) * 100:.1f}%")
        
        # 因子权重分布
        print(f"\n{'='*70}")
        print("因子使用频率统计")
        print(f"{'='*70}")
        
        factor_usage = {}
        for r in results:
            for f in r['optimal_weights'].keys():
                factor_usage[f] = factor_usage.get(f, 0) + 1
        
        for f, count in sorted(factor_usage.items(), key=lambda x: -x[1]):
            print(f"{f:8s}: {count:3d}只股票使用 ({count/len(results)*100:.0f}%)")
        
        # Top 10
        top_10 = df_results.nlargest(10, 'annual_return')
        
        print(f"\nTop 10 股票及其最优权重:")
        print(f"{'='*70}")
        
        for _, row in top_10.iterrows():
            print(f"\n{row['stock_code']} - 年化{row['annual_return']}% 夏普{row['sharpe_ratio']}")
            weights_str = ", ".join([f"{k}:{v:.2f}" for k, v in row['optimal_weights'].items()])
            print(f"  权重: {weights_str}")
        
        # 保存
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_count': len(results),
            'summary': {
                'avg_return': round(df_results['annual_return'].mean(), 2),
                'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
                'avg_drawdown': round(df_results['max_drawdown'].mean(), 2),
                'beat_benchmark_pct': round((df_results['excess_return'] > 0).sum() / len(df_results) * 100, 1)
            },
            'factor_usage': factor_usage,
            'stock_weights': {r['stock_code']: r['optimal_weights'] for r in results},
            'all_results': results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"stock_level_weights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = StockLevelOptimizer()
    return optimizer.optimize_all(stock_limit=50)


if __name__ == "__main__":
    main()