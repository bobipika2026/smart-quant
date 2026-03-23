"""
终极优化版本：市场择时 + 股票级别定制权重 + 动态仓位

结合三种优化：
1. 市场择时：识别牛/熊/震荡
2. 股票定制权重：每只股票使用最优因子权重
3. 动态仓位：波动率 + 信号强度 + 市场环境
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class UltimateOptimizer:
    """终极优化器"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 市场环境仓位调整
    REGIME_POSITION_ADJ = {
        'bull': 1.3,      # 牛市加仓30%
        'bear': 0.3,      # 熊市减仓70%
        'sideways': 0.8,  # 震荡适中
    }
    
    # 基础仓位
    BASE_POSITION = 0.6
    
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
    
    def calculate_all_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有因子"""
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
    
    def calculate_stock_ic_weights(self, df: pd.DataFrame) -> Dict[str, float]:
        """计算股票级别的IC权重"""
        factor_ics = {}
        
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'ROE', 'TURN', 'EP', 'BP']:
            if factor not in df.columns:
                continue
            
            # 计算IC（与未来20日收益的相关性）
            factor_series = df[factor]
            future_return = df['close'].pct_change(20).shift(-20)
            
            valid = pd.DataFrame({'f': factor_series, 'r': future_return}).dropna()
            
            if len(valid) < 100:
                factor_ics[factor] = 0.0
                continue
            
            try:
                from scipy.stats import spearmanr
                ic, _ = spearmanr(valid['f'], valid['r'])
                factor_ics[factor] = ic if not np.isnan(ic) else 0.0
            except:
                factor_ics[factor] = 0.0
        
        # 转换为权重
        total_ic = sum([abs(ic) for ic in factor_ics.values()])
        
        if total_ic > 0:
            weights = {f: round(abs(ic) / total_ic, 3) for f, ic in factor_ics.items()}
            # 只保留有效因子
            weights = {f: w for f, w in weights.items() if abs(factor_ics.get(f, 0)) > 0.01}
            # 归一化
            total = sum(weights.values())
            if total > 0:
                weights = {f: round(w / total, 3) for f, w in weights.items()}
        else:
            # 默认权重
            weights = {'EP': 0.25, 'BP': 0.25, 'ROE': 0.2, 'LEV': 0.15, 'MOM': 0.15}
        
        return weights
    
    def detect_market_regime(self, df: pd.DataFrame) -> str:
        """识别市场环境"""
        if len(df) < 60:
            return 'sideways'
        
        # 趋势
        ma_short = df['close'].rolling(20).mean().iloc[-1]
        ma_long = df['close'].rolling(60).mean().iloc[-1]
        trend = (ma_short / ma_long - 1) * 100
        
        # 收益
        ret_60 = (df['close'].iloc[-1] / df['close'].iloc[-60] - 1) * 100
        
        if trend > 2 and ret_60 > 5:
            return 'bull'
        elif trend < -2 and ret_60 < -5:
            return 'bear'
        else:
            return 'sideways'
    
    def run_ultimate_backtest(self, stock_code: str) -> Dict:
        """终极回测"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 1500:
                return None
            
            # 计算因子
            df = self.calculate_all_factors(df)
            
            # 步骤1：计算股票级别权重
            stock_weights = self.calculate_stock_ic_weights(df)
            
            # 步骤2：计算加权得分
            df['score'] = 0
            for factor, weight in stock_weights.items():
                if factor in df.columns:
                    factor_std = (df[factor] - df[factor].rolling(252).mean()) / \
                                (df[factor].rolling(252).std() + 0.0001)
                    df['score'] += factor_std.fillna(0) * weight
            
            df['score_z'] = (df['score'] - df['score'].rolling(252).mean()) / \
                           (df['score'].rolling(252).std() + 0.0001)
            
            # 步骤3：市场择时（滚动识别）
            df['regime'] = 'sideways'
            for i in range(60, len(df)):
                regime = self.detect_market_regime(df.iloc[:i])
                df.iloc[i, df.columns.get_loc('regime')] = regime
            
            # 步骤4：动态仓位
            df['signal'] = 0
            df.loc[df['score_z'] > 0.3, 'signal'] = 1
            df.loc[df['score_z'] < -0.3, 'signal'] = -1
            
            # 波动率调整
            vol = df['close'].pct_change().rolling(20).std() * np.sqrt(252)
            vol_adj = 0.2 / (vol + 0.0001)
            vol_adj = vol_adj.clip(0.3, 1.0)
            
            # 市场环境调整
            regime_adj = df['regime'].map(self.REGIME_POSITION_ADJ).fillna(0.8)
            
            # 信号强度调整
            signal_strength = df['score_z'].abs()
            signal_adj = np.where(signal_strength > 1.0, 1.2,
                                 np.where(signal_strength > 0.5, 1.0, 0.8))
            
            # 综合仓位
            df['position'] = self.BASE_POSITION * vol_adj * regime_adj * signal_adj
            df['position'] = df['position'].clip(0, 0.95)
            df['position'] = df['position'] * df['signal'].abs()
            
            # 计算收益
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['position'].shift(1) * df['returns']
            df = df.dropna()
            
            if len(df) < 500:
                return None
            
            # 绩效
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
            
            trades = df[df['position'] > 0]
            win_rate = (trades['strategy_returns'] > 0).sum() / len(trades) * 100 if len(trades) > 0 else 0
            
            # 短期验证
            short_results = []
            for j in range(6):
                start_idx = len(df) - 120 + j * 20
                end_idx = start_idx + 20
                if start_idx >= 0 and end_idx <= len(df):
                    period = df.iloc[start_idx:end_idx]
                    if len(period) > 0:
                        period_return = (1 + period['strategy_returns']).prod() - 1
                        short_results.append(period_return)
            
            short_return = np.mean(short_results) * 100 if short_results else 0
            short_win = sum([r > 0 for r in short_results]) / len(short_results) * 100 if short_results else 0
            
            # 市场环境分布
            regime_dist = df['regime'].value_counts().to_dict()
            
            return {
                'stock_code': stock_code,
                'optimal_weights': stock_weights,
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'excess_return': round((annual_return - benchmark_annual) * 100, 2),
                'win_rate': round(win_rate, 1),
                'short_term_return': round(short_return, 2),
                'short_term_winrate': round(short_win, 1),
                'regime_distribution': regime_dist,
                'trading_days': len(df)
            }
            
        except Exception as e:
            return None
    
    def optimize(self, stock_limit: int = 50) -> Dict:
        """终极优化"""
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("终极优化：市场择时 + 股票定制权重 + 动态仓位")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"优化内容:")
        print(f"  1. 市场择时：识别牛/熊/震荡，调整仓位")
        print(f"  2. 股票定制：根据IC为每只股票定制因子权重")
        print(f"  3. 动态仓位：波动率 + 信号强度 + 市场环境")
        print(f"{'='*70}\n")
        
        results = []
        
        for i, stock in enumerate(stocks, 1):
            if i % 10 == 0:
                print(f"进度: {i}/{len(stocks)}")
                sys.stdout.flush()
            
            result = self.run_ultimate_backtest(stock)
            if result:
                results.append(result)
        
        if not results:
            return {'status': 'no_results'}
        
        df_results = pd.DataFrame(results)
        
        # 汇总
        summary = {
            'total_stocks': len(stocks),
            'success_count': len(results),
            'avg_return': round(df_results['annual_return'].mean(), 2),
            'median_return': round(df_results['annual_return'].median(), 2),
            'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
            'avg_drawdown': round(df_results['max_drawdown'].mean(), 2),
            'avg_winrate': round(df_results['win_rate'].mean(), 1),
            'avg_short_return': round(df_results['short_term_return'].mean(), 2),
            'avg_short_winrate': round(df_results['short_term_winrate'].mean(), 1),
            'beat_benchmark_pct': round((df_results['excess_return'] > 0).sum() / len(df_results) * 100, 1),
            'positive_return_pct': round((df_results['annual_return'] > 0).sum() / len(df_results) * 100, 1)
        }
        
        print(f"\n{'='*70}")
        print("终极优化结果")
        print(f"{'='*70}")
        print(f"成功回测: {summary['success_count']}/{summary['total_stocks']}")
        print(f"\n长期验证:")
        print(f"  平均年化收益: {summary['avg_return']}%")
        print(f"  中位数收益: {summary['median_return']}%")
        print(f"  平均夏普比率: {summary['avg_sharpe']}")
        print(f"  平均最大回撤: {summary['avg_drawdown']}%")
        print(f"  正收益占比: {summary['positive_return_pct']}%")
        print(f"  跑赢基准占比: {summary['beat_benchmark_pct']}%")
        print(f"\n短期验证:")
        print(f"  短期收益: {summary['avg_short_return']}%")
        print(f"  短期胜率: {summary['avg_short_winrate']}%")
        
        # Top 10
        top_10 = df_results.nlargest(10, 'annual_return')[
            ['stock_code', 'annual_return', 'sharpe_ratio', 'max_drawdown', 'win_rate', 'optimal_weights']
        ]
        
        print(f"\nTop 10 表现最好:")
        print("-" * 70)
        
        for _, row in top_10.iterrows():
            weights_str = ", ".join([f"{k}:{v:.0%}" for k, v in list(row['optimal_weights'].items())[:4]])
            print(f"{row['stock_code']:12s} 年化{row['annual_return']:>6.2f}% 夏普{row['sharpe_ratio']:>5.2f} "
                  f"回撤{row['max_drawdown']:>6.1f}% 胜率{row['win_rate']:>5.1f}%")
            print(f"             权重: {weights_str}")
        
        # 保存
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'optimization': {
                'market_timing': '牛/熊/震荡识别，仓位调整',
                'stock_weights': '根据IC定制每只股票因子权重',
                'dynamic_position': '基础仓位×波动率×市场环境×信号强度'
            },
            'regime_position_adj': self.REGIME_POSITION_ADJ,
            'summary': summary,
            'stock_weights': {r['stock_code']: r['optimal_weights'] for r in results},
            'top_10': top_10[['stock_code', 'annual_return', 'sharpe_ratio', 'optimal_weights']].to_dict('records'),
            'all_results': results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"ultimate_optimization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = UltimateOptimizer()
    return optimizer.optimize(stock_limit=50)


if __name__ == "__main__":
    main()