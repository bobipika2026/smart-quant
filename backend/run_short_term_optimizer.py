"""
短期验证因子组合优化

目标：
1. 针对现有股票池（50只）
2. 找到最优因子组合
3. 短期验证（1-3个月）

方法：
- 遍历因子组合，找出短期表现最优的组合
- 使用滚动窗口验证
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
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class ShortTermFactorOptimizer:
    """短期因子组合优化器"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 可用因子列表（筛选后，减少到10个核心因子）
    AVAILABLE_FACTORS = [
        'KDJ_14_3_3', 'BOLL_20_2', 'VOL_M_5_60', 'MOM3',
        'TURN', 'LEV', 'EP', 'BP', 'EPS_G', 'ROA'
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
            return [os.path.basename(f).replace("_day.csv", "").replace(".SZ", "").replace(".SH", "") for f in files[:limit]]
    
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
    
    def calculate_all_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有因子"""
        result = df.copy()
        
        # 技术因子
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        result['KDJ_14_3_3'] = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL_20_2'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        vol_ma5 = result['volume'].rolling(5).mean()
        vol_ma60 = result['volume'].rolling(60).mean()
        result['VOL_M_5_60'] = vol_ma5 / (vol_ma60 + 0.0001) - 1
        
        result['MOM3'] = result['close'].pct_change(63)
        result['ATR_7'] = result['close'].pct_change().rolling(7).std()
        
        # 情绪因子
        result['TURN'] = result['volume'] / (result['close'] + 0.0001)
        result['VOL_20'] = result['close'].pct_change().rolling(20).std()
        
        # 基本面因子（简化计算）
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        result['DIV_YIELD'] = 0.02
        
        # 成长因子
        result['EPS_G'] = result['close'].pct_change(63)
        result['ROE_D'] = result['close'].pct_change(126)
        
        # 质量因子
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        result['ROA'] = result['close'].pct_change().rolling(60).mean()
        result['GPM'] = result['close'] / (result['close'].rolling(60).mean() + 0.0001) - 1
        
        return result
    
    def test_factor_combination(self, stock_code: str, factor_combo: Tuple[str], 
                                  lookback: int = 252, holding: int = 20) -> Dict:
        """测试单个因子组合"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < lookback + holding:
                return None
            
            df = self.calculate_all_factors(df)
            
            # 计算组合得分
            df['score'] = 0
            for factor in factor_combo:
                if factor in df.columns:
                    df['score'] += df[factor].fillna(0)
            
            # 标准化
            df['score_z'] = (df['score'] - df['score'].rolling(lookback).mean()) / \
                           (df['score'].rolling(lookback).std() + 0.0001)
            
            # 短期验证：滚动测试最近N个周期
            test_periods = min(6, len(df) - lookback - holding)
            if test_periods < 3:
                return None
            
            returns_list = []
            for i in range(test_periods):
                test_start = len(df) - holding - (i + 1) * 20
                test_end = test_start + holding
                
                if test_start < lookback:
                    continue
                
                # 在test_start位置生成信号
                signal = 1 if df['score_z'].iloc[test_start] > 0.5 else (-1 if df['score_z'].iloc[test_start] < -0.5 else 0)
                
                # 计算持有期收益
                period_return = (df['close'].iloc[test_end] / df['close'].iloc[test_start] - 1) * signal
                returns_list.append(period_return)
            
            if len(returns_list) < 3:
                return None
            
            avg_return = np.mean(returns_list)
            win_rate = sum([r > 0 for r in returns_list]) / len(returns_list)
            
            return {
                'stock_code': stock_code,
                'factors': list(factor_combo),
                'n_factors': len(factor_combo),
                'avg_return': round(avg_return * 100, 2),
                'win_rate': round(win_rate * 100, 1),
                'test_periods': len(returns_list)
            }
            
        except Exception as e:
            return None
    
    def optimize(self, stock_limit: int = 50, min_factors: int = 2, max_factors: int = 4) -> Dict:
        """
        优化因子组合
        
        Args:
            stock_limit: 股票数量
            min_factors: 最小因子数
            max_factors: 最大因子数
        """
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("短期因子组合优化")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"因子范围: {min_factors}-{max_factors}个")
        print(f"验证周期: 20天持有期，滚动6个周期")
        print(f"{'='*70}\n")
        
        all_results = []
        combo_count = 0
        total_combos = sum([len(list(combinations(self.AVAILABLE_FACTORS, n))) 
                           for n in range(min_factors, max_factors + 1)])
        
        print(f"总组合数: {total_combos}")
        print(f"开始测试...\n")
        sys.stdout.flush()
        
        for n_factors in range(min_factors, max_factors + 1):
            print(f"\n测试 {n_factors} 因子组合...")
            sys.stdout.flush()
            
            for factor_combo in combinations(self.AVAILABLE_FACTORS, n_factors):
                combo_count += 1
                
                if combo_count % 50 == 0:
                    print(f"进度: {combo_count}/{total_combos}")
                    sys.stdout.flush()
                
                # 在所有股票上测试这个组合
                combo_returns = []
                combo_wins = 0
                combo_tests = 0
                
                for stock in stocks:
                    result = self.test_factor_combination(stock, factor_combo)
                    if result:
                        combo_returns.append(result['avg_return'])
                        if result['avg_return'] > 0:
                            combo_wins += 1
                        combo_tests += 1
                
                if combo_tests >= len(stocks) * 0.5:  # 至少一半股票有结果
                    avg_return = np.mean(combo_returns)
                    win_rate = combo_wins / combo_tests * 100
                    
                    all_results.append({
                        'factors': list(factor_combo),
                        'n_factors': n_factors,
                        'avg_return': round(avg_return, 2),
                        'win_rate': round(win_rate, 1),
                        'tested_stocks': combo_tests
                    })
        
        # 排序找出最优组合
        results_df = pd.DataFrame(all_results)
        results_df = results_df.sort_values('avg_return', ascending=False)
        
        print(f"\n{'='*70}")
        print("优化结果")
        print(f"{'='*70}")
        
        # Top 20 组合
        top_20 = results_df.head(20)
        
        print(f"\nTop 20 因子组合:")
        print("-" * 70)
        for i, row in top_20.iterrows():
            factors_str = '+'.join(row['factors'])
            print(f"{row['n_factors']}因子: {factors_str[:50]:<50s} 收益{row['avg_return']:>6.2f}% 胜率{row['win_rate']:>5.1f}%")
        
        # 最佳组合
        best = results_df.iloc[0]
        
        print(f"\n{'='*70}")
        print("🏆 最优因子组合")
        print(f"{'='*70}")
        print(f"因子: {', '.join(best['factors'])}")
        print(f"平均收益: {best['avg_return']}%")
        print(f"胜率: {best['win_rate']}%")
        print(f"测试股票数: {best['tested_stocks']}")
        
        # 按因子数分组统计
        print(f"\n按因子数统计:")
        print("-" * 40)
        for n in range(min_factors, max_factors + 1):
            subset = results_df[results_df['n_factors'] == n]
            if len(subset) > 0:
                print(f"{n}因子: 平均收益 {subset['avg_return'].mean():.2f}%, 最佳 {subset['avg_return'].max():.2f}%")
        
        # 保存结果
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_pool': len(stocks),
            'validation_period': '20天持有期',
            'rolling_periods': 6,
            'best_combination': {
                'factors': best['factors'],
                'avg_return': best['avg_return'],
                'win_rate': best['win_rate']
            },
            'top_20': top_20.to_dict('records'),
            'all_results': results_df.to_dict('records')
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"short_term_optimal_factors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = ShortTermFactorOptimizer()
    return optimizer.optimize(stock_limit=50, min_factors=3, max_factors=5)


if __name__ == "__main__":
    main()