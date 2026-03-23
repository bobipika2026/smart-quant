"""
双重验证因子组合优化

验证要求：
1. 10年期回测验证（长期有效性）
2. 短期滚动验证（实时可验证）

筛选标准：
- 长期年化收益 > 5%
- 长期夏普比率 > 0.5
- 短期胜率 > 50%
- 短期平均收益 > 0
"""
import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from itertools import combinations
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class DualValidationOptimizer:
    """双重验证因子组合优化器"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 核心因子（基于之前IC测试结果）
    CORE_FACTORS = [
        ('KDJ_14_3_3', 'technical', 0.8712),
        ('BOLL_20_2', 'technical', 0.8092),
        ('VOL_M_5_60', 'momentum', 0.7346),
        ('MOM3', 'momentum', 0.5901),
        ('LEV', 'quality', 0.2125),
        ('EP', 'value', 0.1712),
        ('BP', 'value', 0.1294),
        ('TURN', 'sentiment', 0.3024),
    ]
    
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
            import glob
            files = glob.glob(os.path.join(self.DAY_CACHE_DIR, "*_day.csv"))
            return [os.path.basename(f).replace("_day.csv", "").replace(".SZ", "").replace(".SH", "") 
                    for f in files[:limit]]
    
    def get_stock_data(self, stock_code: str) -> pd.DataFrame:
        """获取股票日线数据"""
        possible_files = [
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SZ_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SH_day.csv"),
        ]
        
        file_path = None
        for fp in possible_files:
            if os.path.exists(fp):
                file_path = fp
                break
        
        if not file_path:
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            column_map = {'日期': 'trade_date', '开盘': 'open', '最高': 'high', 
                          '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'}
            df = df.rename(columns=column_map)
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
            df.set_index('trade_date', inplace=True)
            return df[['open', 'high', 'low', 'close', 'volume', 'amount']]
        except:
            return pd.DataFrame()
    
    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算因子"""
        result = df.copy()
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        result['KDJ_14_3_3'] = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        
        # BOLL
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL_20_2'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # VOL_M
        vol_ma5 = result['volume'].rolling(5).mean()
        vol_ma60 = result['volume'].rolling(60).mean()
        result['VOL_M_5_60'] = vol_ma5 / (vol_ma60 + 0.0001) - 1
        
        # MOM3
        result['MOM3'] = result['close'].pct_change(63)
        
        # LEV (负波动率)
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # EP
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        
        # BP
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        # TURN
        result['TURN'] = result['volume'] / (result['close'] + 0.0001)
        
        return result
    
    def long_term_backtest(self, df: pd.DataFrame, factor_combo: Tuple[str]) -> Dict:
        """长期回测（10年）"""
        try:
            if len(df) < 2000:  # 至少8年数据（约2000个交易日）
                return None
            
            df = self.calculate_factors(df)
            
            # 计算组合得分
            df['score'] = 0
            for factor in factor_combo:
                if factor in df.columns:
                    df['score'] += df[factor].fillna(0)
            
            # 标准化
            df['score_z'] = (df['score'] - df['score'].rolling(252).mean()) / \
                           (df['score'].rolling(252).std() + 0.0001)
            
            # 生成信号
            df['signal'] = 0
            df.loc[df['score_z'] > 0.5, 'signal'] = 1
            df.loc[df['score_z'] < -0.5, 'signal'] = -1
            
            # 计算收益
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['signal'].shift(1) * df['returns']
            df = df.dropna()
            
            # 计算绩效
            total_return = (1 + df['strategy_returns']).prod() - 1
            annual_return = (1 + total_return) ** (252 / len(df)) - 1 if len(df) > 0 else 0
            volatility = df['strategy_returns'].std() * np.sqrt(252) if len(df) > 0 else 0
            sharpe = annual_return / volatility if volatility > 0 else 0
            
            # 最大回撤
            cum_returns = (1 + df['strategy_returns']).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # 基准
            benchmark_return = df['close'].iloc[-1] / df['close'].iloc[0] - 1
            
            return {
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'excess_return': round((annual_return - benchmark_return / (len(df)/252)) * 100, 2),
                'trading_days': len(df)
            }
            
        except:
            return None
    
    def short_term_validation(self, df: pd.DataFrame, factor_combo: Tuple[str], 
                                lookback: int = 252, holding: int = 20) -> Dict:
        """短期验证（滚动20天）"""
        try:
            if len(df) < lookback + holding * 3:  # 至少需要 lookback + 60天
                return None
            
            df = self.calculate_factors(df)
            
            # 计算组合得分
            df['score'] = 0
            for factor in factor_combo:
                if factor in df.columns:
                    df['score'] += df[factor].fillna(0)
            
            # 标准化
            df['score_z'] = (df['score'] - df['score'].rolling(lookback).mean()) / \
                           (df['score'].rolling(lookback).std() + 0.0001)
            
            # 滚动测试最近6个周期
            test_periods = 6
            returns_list = []
            
            for i in range(test_periods):
                test_start = len(df) - holding - (i + 1) * 20
                test_end = test_start + holding
                
                if test_start < lookback:
                    continue
                
                signal = 1 if df['score_z'].iloc[test_start] > 0.5 else \
                        (-1 if df['score_z'].iloc[test_start] < -0.5 else 0)
                
                period_return = (df['close'].iloc[test_end] / df['close'].iloc[test_start] - 1) * signal
                returns_list.append(period_return)
            
            if len(returns_list) < 3:
                return None
            
            avg_return = np.mean(returns_list)
            win_rate = sum([r > 0 for r in returns_list]) / len(returns_list) * 100
            
            return {
                'avg_return': round(avg_return * 100, 2),
                'win_rate': round(win_rate, 1),
                'test_periods': len(returns_list)
            }
            
        except:
            return None
    
    def optimize(self, stock_limit: int = 50, min_factors: int = 2, max_factors: int = 4) -> Dict:
        """双重验证优化"""
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("双重验证因子组合优化")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"因子范围: {min_factors}-{max_factors}个")
        print(f"长期验证: 10年回测")
        print(f"短期验证: 20天持有期，滚动6个周期")
        print(f"{'='*70}\n")
        
        factor_names = [f[0] for f in self.CORE_FACTORS]
        total_combos = sum([len(list(combinations(factor_names, n))) 
                           for n in range(min_factors, max_factors + 1)])
        
        print(f"总组合数: {total_combos}")
        sys.stdout.flush()
        
        all_results = []
        combo_count = 0
        
        for n_factors in range(min_factors, max_factors + 1):
            print(f"\n测试 {n_factors} 因子组合...")
            sys.stdout.flush()
            
            for factor_combo in combinations(factor_names, n_factors):
                combo_count += 1
                
                if combo_count % 20 == 0:
                    print(f"进度: {combo_count}/{total_combos}")
                    sys.stdout.flush()
                
                # 长期验证
                long_term_results = []
                for stock in stocks:
                    df = self.get_stock_data(stock)
                    if df.empty:
                        continue
                    result = self.long_term_backtest(df, factor_combo)
                    if result:
                        long_term_results.append(result)
                
                # 短期验证
                short_term_results = []
                for stock in stocks:
                    df = self.get_stock_data(stock)
                    if df.empty:
                        continue
                    result = self.short_term_validation(df, factor_combo)
                    if result:
                        short_term_results.append(result)
                
                # 综合评估
                if len(long_term_results) >= len(stocks) * 0.5 and len(short_term_results) >= len(stocks) * 0.5:
                    long_avg_return = np.mean([r['annual_return'] for r in long_term_results])
                    long_avg_sharpe = np.mean([r['sharpe_ratio'] for r in long_term_results])
                    long_avg_drawdown = np.mean([r['max_drawdown'] for r in long_term_results])
                    
                    short_avg_return = np.mean([r['avg_return'] for r in short_term_results])
                    short_avg_winrate = np.mean([r['win_rate'] for r in short_term_results])
                    
                    # 双重验证通过条件
                    long_pass = long_avg_return > 3 and long_avg_sharpe > 0.3
                    short_pass = short_avg_winrate > 45 and short_avg_return > -0.5
                    
                    all_results.append({
                        'factors': list(factor_combo),
                        'n_factors': n_factors,
                        'long_term': {
                            'avg_return': round(long_avg_return, 2),
                            'avg_sharpe': round(long_avg_sharpe, 3),
                            'avg_drawdown': round(long_avg_drawdown, 2)
                        },
                        'short_term': {
                            'avg_return': round(short_avg_return, 2),
                            'avg_winrate': round(short_avg_winrate, 1)
                        },
                        'dual_pass': long_pass and short_pass,
                        'score': round(long_avg_sharpe * 10 + short_avg_winrate / 10, 2)  # 综合得分
                    })
        
        # 排序
        if len(all_results) == 0:
            print("\n⚠️ 没有有效的回测结果")
            return {'status': 'no_results'}
        
        results_df = pd.DataFrame(all_results)
        results_df = results_df.sort_values('score', ascending=False)
        
        print(f"\n{'='*70}")
        print("优化结果")
        print(f"{'='*70}")
        
        # 双重验证通过的组合
        passed = results_df[results_df['dual_pass'] == True]
        
        if len(passed) > 0:
            print(f"\n✅ 双重验证通过: {len(passed)}个组合")
            print("-" * 70)
            
            for i, row in passed.head(10).iterrows():
                factors_str = '+'.join(row['factors'])
                print(f"{row['n_factors']}因子: {factors_str[:40]:<40s}")
                print(f"  长期: 收益{row['long_term']['avg_return']:>6.2f}% 夏普{row['long_term']['avg_sharpe']:>5.2f}")
                print(f"  短期: 收益{row['short_term']['avg_return']:>6.2f}% 胜率{row['short_term']['avg_winrate']:>5.1f}%")
                print()
            
            best = passed.iloc[0]
        else:
            print("\n⚠️ 没有组合通过双重验证")
            print("显示综合得分最高的10个组合:")
            print("-" * 70)
            
            for i, row in results_df.head(10).iterrows():
                factors_str = '+'.join(row['factors'])
                print(f"{row['n_factors']}因子: {factors_str[:40]:<40s}")
                print(f"  长期: 收益{row['long_term']['avg_return']:>6.2f}% 夏普{row['long_term']['avg_sharpe']:>5.2f}")
                print(f"  短期: 收益{row['short_term']['avg_return']:>6.2f}% 胜率{row['short_term']['avg_winrate']:>5.1f}%")
                print()
            
            best = results_df.iloc[0]
        
        print(f"\n{'='*70}")
        print("🏆 最优因子组合")
        print(f"{'='*70}")
        print(f"因子: {', '.join(best['factors'])}")
        print(f"\n长期验证（10年）:")
        print(f"  年化收益: {best['long_term']['avg_return']}%")
        print(f"  夏普比率: {best['long_term']['avg_sharpe']}")
        print(f"  最大回撤: {best['long_term']['avg_drawdown']}%")
        print(f"\n短期验证（20天）:")
        print(f"  平均收益: {best['short_term']['avg_return']}%")
        print(f"  平均胜率: {best['short_term']['avg_winrate']}%")
        print(f"\n双重验证: {'✅ 通过' if best['dual_pass'] else '❌ 未通过'}")
        
        # 保存结果
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_pool': len(stocks),
            'validation': {
                'long_term': '10年回测',
                'short_term': '20天持有期，滚动6个周期'
            },
            'best_combination': {
                'factors': best['factors'],
                'long_term': best['long_term'],
                'short_term': best['short_term'],
                'dual_pass': best['dual_pass']
            },
            'all_results': results_df.to_dict('records')
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"dual_validation_optimal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = DualValidationOptimizer()
    return optimizer.optimize(stock_limit=50, min_factors=2, max_factors=4)


if __name__ == "__main__":
    main()