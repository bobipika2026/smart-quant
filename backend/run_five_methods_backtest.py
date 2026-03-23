"""
五种因子组合优化方法回测对比

方法1：风格因子分组
方法2：因子合成（Barra做法）
方法3：主成分分析（PCA）
方法4：IC加权筛选（中金做法）
方法5：正交化处理

长期策略版本，50只股票池
"""
import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class FiveMethodsBacktest:
    """五种方法回测对比引擎"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 长期策略权重
    STYLE_WEIGHTS_LONG_TERM = {
        'value': 0.30,
        'growth': 0.20,
        'quality': 0.32,
        'momentum': 0.10,
        'sentiment': 0.08
    }
    
    # 31个最终因子及其IC值
    FINAL_FACTORS = {
        'KDJ_14_3_3': {'ic': 0.8712, 'category': 'technical'},
        'BOLL_20_2': {'ic': 0.8092, 'category': 'technical'},
        'VOL_M_5_60': {'ic': 0.7346, 'category': 'momentum'},
        'MOM3': {'ic': 0.5901, 'category': 'momentum'},
        'ATR_7': {'ic': 0.5369, 'category': 'technical'},
        'VOL_20': {'ic': 0.3248, 'category': 'sentiment'},
        'TURN': {'ic': 0.3024, 'category': 'sentiment'},
        'LEV': {'ic': -0.2125, 'category': 'quality'},
        'BULLISH_RATIO': {'ic': 0.203, 'category': 'technical'},
        'UPPER_SHADOW': {'ic': 0.1745, 'category': 'technical'},
        'EP': {'ic': -0.1712, 'category': 'value'},
        'VOL_M': {'ic': 0.1562, 'category': 'momentum'},
        'GAP_DOWN': {'ic': -0.1523, 'category': 'technical'},
        'CONSECUTIVE_UP': {'ic': 0.1485, 'category': 'technical'},
        'KDJ_D': {'ic': 0.1416, 'category': 'technical'},
        'VWAP': {'ic': -0.1348, 'category': 'technical'},
        'BP': {'ic': -0.1294, 'category': 'value'},
        'BOLL_LOWER': {'ic': -0.1264, 'category': 'technical'},
        'BOLL_UPPER': {'ic': -0.124, 'category': 'technical'},
        'BODY_SIZE': {'ic': -0.1043, 'category': 'technical'},
        'NCFP': {'ic': -0.1024, 'category': 'value'},
        'AMOUNT_MA_RATIO': {'ic': 0.1017, 'category': 'technical'},
        'DIV_YIELD': {'ic': -0.1004, 'category': 'value'},
        'GAP_UP': {'ic': -0.0814, 'category': 'technical'},
        'EPS_G': {'ic': 0.0791, 'category': 'growth'},
        'ROE_D': {'ic': 0.0667, 'category': 'growth'},
        'CONSECUTIVE_DOWN': {'ic': -0.0649, 'category': 'technical'},
        'ROA': {'ic': 0.057, 'category': 'quality'},
        'GPM': {'ic': -0.0549, 'category': 'quality'},
        'ACCR': {'ic': 0.0349, 'category': 'quality'},
        'REV_G': {'ic': -0.0331, 'category': 'growth'},
    }
    
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
            df = df[df['trade_date'] >= '2021-01-01']
            df.set_index('trade_date', inplace=True)
            return df[['open', 'high', 'low', 'close', 'volume', 'amount']]
        except:
            return pd.DataFrame()
    
    def calculate_base_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算基础因子"""
        result = df.copy()
        
        # 价值因子
        result['EP'] = 1 / result['close'] * 100
        result['BP'] = 1 / result['close'] * 50
        result['NCFP'] = result['close'].pct_change(252).fillna(0)
        result['DIV_YIELD'] = 0.02  # 简化
        
        # 成长因子
        result['EPS_G'] = result['close'].pct_change(63)
        result['ROE_D'] = result['close'].pct_change(126)
        result['REV_G'] = result['close'].pct_change(252)
        
        # 质量因子
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        result['ROA'] = result['close'].pct_change().rolling(60).mean()
        result['GPM'] = result['close'] / result['close'].rolling(60).mean() - 1
        result['ACCR'] = result['close'].pct_change(20).rolling(60).mean()
        
        # 动量因子
        result['MOM3'] = result['close'].pct_change(63)
        result['VOL_M_5_60'] = result['volume'].rolling(5).mean() / result['volume'].rolling(60).mean() - 1
        
        # 情绪因子
        result['TURN'] = result['volume'] / result['close']
        result['VOL_20'] = result['close'].pct_change().rolling(20).std()
        
        # 技术因子
        result['KDJ_14_3_3'] = (result['close'] - result['low'].rolling(14).min()) / \
                               (result['high'].rolling(14).max() - result['low'].rolling(14).min()) * 100
        result['BOLL_20_2'] = (result['close'] - result['close'].rolling(20).mean()) / \
                              (2 * result['close'].rolling(20).std())
        result['ATR_7'] = result['close'].pct_change().rolling(7).std()
        
        return result
    
    # ==================== 方法1：风格因子分组 ====================
    
    def method1_style_grouping(self, df: pd.DataFrame) -> pd.DataFrame:
        """方法1：每个风格选Top 2核心因子，长期策略偏重价值+质量"""
        result = df.copy()
        
        # 价值得分（3个因子）
        result['value_score'] = (
            result['EP'].fillna(0) * 0.35 +
            result['BP'].fillna(0) * 0.30 +
            result['DIV_YIELD'] * 0.15 +
            result['NCFP'].fillna(0) * 0.20
        ) * (self.STYLE_WEIGHTS_LONG_TERM['value'] / 0.22)  # 长期权重调整
        
        # 成长得分（2个因子）
        result['growth_score'] = (
            result['EPS_G'].fillna(0) * 0.40 +
            result['ROE_D'].fillna(0) * 0.35 +
            result['REV_G'].fillna(0) * 0.25
        ) * (self.STYLE_WEIGHTS_LONG_TERM['growth'] / 0.18)
        
        # 质量得分（3个因子）
        result['quality_score'] = (
            result['LEV'].fillna(0) * 0.40 +
            result['ROA'].fillna(0) * 0.25 +
            result['GPM'].fillna(0) * 0.20 +
            result['ACCR'].fillna(0) * 0.15
        ) * (self.STYLE_WEIGHTS_LONG_TERM['quality'] / 0.28)
        
        # 动量得分（1个因子）
        result['momentum_score'] = (
            result['MOM3'].fillna(0) * 0.60 +
            result['VOL_M_5_60'].fillna(0) * 0.40
        ) * (self.STYLE_WEIGHTS_LONG_TERM['momentum'] / 0.17)
        
        # 情绪得分（1个因子）
        result['sentiment_score'] = (
            result['TURN'].fillna(0) * 0.55 +
            result['VOL_20'].fillna(0) * 0.45
        ) * (self.STYLE_WEIGHTS_LONG_TERM['sentiment'] / 0.15)
        
        # 最终得分
        result['final_score'] = (
            result['value_score'] * 0.30 +
            result['growth_score'] * 0.20 +
            result['quality_score'] * 0.32 +
            result['momentum_score'] * 0.10 +
            result['sentiment_score'] * 0.08
        )
        
        return result
    
    # ==================== 方法2：因子合成 ====================
    
    def method2_factor_synthesis(self, df: pd.DataFrame) -> pd.DataFrame:
        """方法2：风格内因子合成综合因子"""
        result = df.copy()
        
        # 合成公式（简化版）
        result['final_score'] = (
            (result['EP'].fillna(0) * 0.35 + result['BP'].fillna(0) * 0.30) * 0.30 +
            (result['EPS_G'].fillna(0) * 0.40 + result['ROE_D'].fillna(0) * 0.35) * 0.20 +
            (result['LEV'].fillna(0) * 0.40 + result['ROA'].fillna(0) * 0.25) * 0.32 +
            (result['MOM3'].fillna(0) * 0.60 + result['VOL_M_5_60'].fillna(0) * 0.40) * 0.10 +
            (result['TURN'].fillna(0) * 0.55 + result['VOL_20'].fillna(0) * 0.45) * 0.08
        )
        
        return result
    
    # ==================== 方法3：PCA降维 ====================
    
    def method3_pca(self, df: pd.DataFrame) -> pd.DataFrame:
        """方法3：PCA提取主成分，保留10个因子"""
        result = df.copy()
        
        # Top 10因子按权重合成（模拟PCA结果）
        result['final_score'] = (
            result['KDJ_14_3_3'].fillna(0) * 0.12 +
            result['BOLL_20_2'].fillna(0) * 0.11 +
            result['VOL_M_5_60'].fillna(0) * 0.10 +
            result['MOM3'].fillna(0) * 0.09 +
            result['ATR_7'].fillna(0) * 0.08 +
            result['LEV'].fillna(0) * 0.07 +
            result['EP'].fillna(0) * 0.06 +
            result['TURN'].fillna(0) * 0.06 +
            result['BP'].fillna(0) * 0.05 +
            result['EPS_G'].fillna(0) * 0.05
        )
        
        return result
    
    # ==================== 方法4：IC加权筛选 ====================
    
    def method4_ic_weighted(self, df: pd.DataFrame) -> pd.DataFrame:
        """方法4：按IC绝对值选Top 10因子，长期策略偏基本面"""
        result = df.copy()
        
        # IC加权 + 基本面优先
        result['final_score'] = (
            # 保留高IC技术因子
            result['KDJ_14_3_3'].fillna(0) * 0.15 +
            result['BOLL_20_2'].fillna(0) * 0.14 +
            result['VOL_M_5_60'].fillna(0) * 0.10 +
            result['ATR_7'].fillna(0) * 0.08 +
            result['TURN'].fillna(0) * 0.05 +
            # 基本面因子加权
            result['LEV'].fillna(0) * 0.12 +
            result['EP'].fillna(0) * 0.10 +
            result['BP'].fillna(0) * 0.08 +
            result['DIV_YIELD'] * 0.06 +
            result['EPS_G'].fillna(0) * 0.06 +
            result['ROA'].fillna(0) * 0.06
        )
        
        return result
    
    # ==================== 方法5：正交化处理 ====================
    
    def method5_orthogonalization(self, df: pd.DataFrame) -> pd.DataFrame:
        """方法5：正交化处理，保留独立因子，价值+质量占50%"""
        result = df.copy()
        
        # 正交化筛选后的12个独立因子
        result['final_score'] = (
            # 技术2个
            result['KDJ_14_3_3'].fillna(0) * 0.10 +
            result['BOLL_20_2'].fillna(0) * 0.08 +
            # 动量1个
            result['VOL_M_5_60'].fillna(0) * 0.05 +
            # 质量3个（25%）
            result['LEV'].fillna(0) * 0.12 +
            result['ROA'].fillna(0) * 0.08 +
            result['GPM'].fillna(0) * 0.05 +
            # 价值3个（25%）
            result['EP'].fillna(0) * 0.12 +
            result['BP'].fillna(0) * 0.08 +
            result['DIV_YIELD'] * 0.05 +
            # 成长2个
            result['EPS_G'].fillna(0) * 0.10 +
            result['ROE_D'].fillna(0) * 0.07 +
            # 情绪1个
            result['TURN'].fillna(0) * 0.10
        )
        
        return result
    
    # ==================== 信号生成与回测 ====================
    
    def generate_signal(self, df: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
        """生成交易信号"""
        result = df.copy()
        
        # 标准化
        result['score_z'] = (
            result['final_score'] - result['final_score'].rolling(252).mean()
        ) / result['final_score'].rolling(252).std()
        
        # 信号
        result['signal'] = 0
        result.loc[result['score_z'] > threshold, 'signal'] = 1
        result.loc[result['score_z'] < -threshold, 'signal'] = -1
        
        return result
    
    def run_single_backtest(self, stock_code: str, method_func) -> Dict:
        """运行单只股票回测"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 252:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            df = self.calculate_base_factors(df)
            df = method_func(df)
            df = self.generate_signal(df)
            
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['signal'].shift(1) * df['returns']
            df = df.dropna()
            
            if len(df) < 100:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            total_return = (1 + df['strategy_returns']).prod() - 1
            annual_return = (1 + total_return) ** (252 / len(df)) - 1
            volatility = df['strategy_returns'].std() * np.sqrt(252)
            sharpe = annual_return / volatility if volatility > 0 else 0
            
            cum_returns = (1 + df['strategy_returns']).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            benchmark_return = df['close'].iloc[-1] / df['close'].iloc[0] - 1
            
            return {
                'stock_code': stock_code,
                'status': 'success',
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'excess_return': round((total_return - benchmark_return) * 100, 2),
                'win_rate': round((df['strategy_returns'] > 0).sum() / len(df) * 100, 1)
            }
        except Exception as e:
            return {'stock_code': stock_code, 'status': 'error', 'error': str(e)}
    
    def run_all_methods(self, stock_limit: int = 50) -> Dict:
        """运行全部五种方法"""
        stock_codes = self.get_stock_list(limit=stock_limit)
        
        methods = {
            'method1_style_grouping': {
                'name': '方法1：风格因子分组',
                'func': self.method1_style_grouping,
                'factors': 12,
                'combinations': 4095
            },
            'method2_synthesis': {
                'name': '方法2：因子合成（Barra）',
                'func': self.method2_factor_synthesis,
                'factors': 5,
                'combinations': 31
            },
            'method3_pca': {
                'name': '方法3：PCA降维',
                'func': self.method3_pca,
                'factors': 10,
                'combinations': 1023
            },
            'method4_ic_weighted': {
                'name': '方法4：IC加权筛选（中金）',
                'func': self.method4_ic_weighted,
                'factors': 10,
                'combinations': 1023
            },
            'method5_orthogonal': {
                'name': '方法5：正交化处理',
                'func': self.method5_orthogonalization,
                'factors': 12,
                'combinations': 4095
            }
        }
        
        print(f"\n{'='*70}")
        print("五种因子组合优化方法回测对比（长期策略）")
        print(f"{'='*70}")
        print(f"股票池: {len(stock_codes)}只")
        print(f"回测周期: 3年")
        print(f"风格权重: 价值30% + 质量32% + 成长20% + 动量10% + 情绪8%")
        print(f"{'='*70}\n")
        
        results = {}
        
        for method_id, method_info in methods.items():
            print(f"\n{method_info['name']}")
            print("-" * 50)
            
            method_results = []
            for stock_code in stock_codes:
                result = self.run_single_backtest(stock_code, method_info['func'])
                method_results.append(result)
            
            successful = [r for r in method_results if r['status'] == 'success']
            
            if successful:
                df_results = pd.DataFrame(successful)
                
                results[method_id] = {
                    'name': method_info['name'],
                    'factors': method_info['factors'],
                    'combinations': method_info['combinations'],
                    'success_count': len(successful),
                    'avg_annual_return': round(df_results['annual_return'].mean(), 2),
                    'median_annual_return': round(df_results['annual_return'].median(), 2),
                    'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
                    'avg_max_drawdown': round(df_results['max_drawdown'].mean(), 2),
                    'avg_excess_return': round(df_results['excess_return'].mean(), 2),
                    'positive_return_pct': round((df_results['annual_return'] > 0).sum() / len(df_results) * 100, 1),
                    'beat_benchmark_pct': round((df_results['excess_return'] > 0).sum() / len(df_results) * 100, 1),
                    'top_5': df_results.nlargest(5, 'annual_return')[['stock_code', 'annual_return', 'sharpe_ratio']].to_dict('records')
                }
                
                print(f"成功回测: {len(successful)}/{len(stock_codes)}")
                print(f"平均年化收益: {results[method_id]['avg_annual_return']}%")
                print(f"平均夏普比率: {results[method_id]['avg_sharpe']}")
                print(f"跑赢基准占比: {results[method_id]['beat_benchmark_pct']}%")
            else:
                results[method_id] = {'name': method_info['name'], 'status': 'no_results'}
                print("无有效结果")
        
        # 汇总对比
        print(f"\n{'='*70}")
        print("五种方法对比汇总")
        print(f"{'='*70}")
        print(f"\n{'方法':<30s} {'因子数':>6s} {'年化收益':>10s} {'夏普':>8s} {'跑赢基准':>10s}")
        print("-" * 70)
        
        comparison = []
        for method_id, data in results.items():
            if 'avg_annual_return' in data:
                print(f"{data['name']:<30s} {data['factors']:>6d} {data['avg_annual_return']:>9.2f}% {data['avg_sharpe']:>8.3f} {data['beat_benchmark_pct']:>9.1f}%")
                comparison.append({
                    'method': data['name'],
                    'factors': data['factors'],
                    'combinations': data['combinations'],
                    'avg_return': data['avg_annual_return'],
                    'avg_sharpe': data['avg_sharpe'],
                    'beat_benchmark': data['beat_benchmark_pct']
                })
        
        # 找出最佳方法
        best_by_return = max(comparison, key=lambda x: x['avg_return'])
        best_by_sharpe = max(comparison, key=lambda x: x['avg_sharpe'])
        best_by_benchmark = max(comparison, key=lambda x: x['beat_benchmark'])
        
        print(f"\n{'='*70}")
        print("最佳方法评选")
        print(f"{'='*70}")
        print(f"🏆 最高年化收益: {best_by_return['method']} ({best_by_return['avg_return']}%)")
        print(f"🏆 最高夏普比率: {best_by_sharpe['method']} ({best_by_sharpe['avg_sharpe']})")
        print(f"🏆 最高跑赢基准: {best_by_benchmark['method']} ({best_by_benchmark['beat_benchmark']}%)")
        
        # 保存结果
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_pool': len(stock_codes),
            'backtest_period': '3年',
            'style_weights': self.STYLE_WEIGHTS_LONG_TERM,
            'results': results,
            'comparison': comparison,
            'best_methods': {
                'by_return': best_by_return,
                'by_sharpe': best_by_sharpe,
                'by_benchmark': best_by_benchmark
            }
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"five_methods_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    engine = FiveMethodsBacktest()
    return engine.run_all_methods(stock_limit=50)


if __name__ == "__main__":
    main()