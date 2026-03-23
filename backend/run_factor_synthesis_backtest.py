"""
方法2：因子合成回测验证

长期策略版本：
- 风格内因子合成
- 长期权重配置（价值30%, 质量32%, 成长20%, 动量10%, 情绪8%）
- 回测周期：3年
"""
import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import json

# 添加路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.factors.factor_library import FactorLibrary, FactorCategory


class FactorSynthesisBacktest:
    """因子合成回测引擎"""
    
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
    
    # 风格内因子合成权重（基于IC）
    FACTOR_SYNTHESIS_WEIGHTS = {
        'value': {
            'EP': 0.35, 'BP': 0.30, 'NCFP': 0.20, 'DIV_YIELD': 0.15
        },
        'growth': {
            'EPS_G': 0.40, 'ROE_D': 0.35, 'REV_G': 0.25
        },
        'quality': {
            'LEV': 0.40, 'ROA': 0.25, 'GPM': 0.20, 'ACCR': 0.15
        },
        'momentum': {
            'MOM3': 0.60, 'VOL_M_5_60': 0.40
        },
        'sentiment': {
            'TURN': 0.55, 'VOL_20': 0.45
        }
    }
    
    def __init__(self):
        self.library = FactorLibrary()
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
    
    def get_stock_list(self, limit: int = 50) -> List[str]:
        """获取股票列表（从best_factor_combinations筛选，限制数量）"""
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
            
            if len(stocks) < limit:
                # 如果不够，从缓存文件补充
                import glob
                files = glob.glob(os.path.join(self.DAY_CACHE_DIR, "*_day.csv"))
                for f in files:
                    basename = os.path.basename(f)
                    code = basename.replace("_day.csv", "").replace(".SZ", "").replace(".SH", "")
                    if code not in stocks:
                        stocks.append(code)
                    if len(stocks) >= limit:
                        break
            
            return stocks[:limit]
        except:
            # 备用方案：从缓存文件
            import glob
            files = glob.glob(os.path.join(self.DAY_CACHE_DIR, "*_day.csv"))
            stocks = []
            for f in files[:limit]:
                basename = os.path.basename(f)
                code = basename.replace("_day.csv", "").replace(".SZ", "").replace(".SH", "")
                stocks.append(code)
            return stocks
    
    def get_stock_data(self, stock_code: str, start_date: str = '2021-01-01') -> pd.DataFrame:
        """获取股票日线数据（从缓存文件）"""
        # 尝试不同的文件名格式
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
            
            # 列名映射
            column_map = {
                '日期': 'trade_date',
                '开盘': 'open',
                '最高': 'high',
                '最低': 'low',
                '收盘': 'close',
                '成交量': 'volume',
                '成交额': 'amount'
            }
            df = df.rename(columns=column_map)
            
            # 日期处理
            df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
            
            # 过滤日期
            start_dt = pd.to_datetime(start_date)
            df = df[df['trade_date'] >= start_dt]
            
            df.set_index('trade_date', inplace=True)
            
            return df[['open', 'high', 'low', 'close', 'volume', 'amount']]
            
        except Exception as e:
            return pd.DataFrame()
    
    def calculate_style_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算风格因子得分"""
        result = df.copy()
        
        # 价值因子
        # EP: 简化计算，用收盘价的倒数近似
        result['EP'] = 1 / result['close'] * 100
        
        # BP: 简化计算
        result['BP'] = 1 / result['close'] * 50
        
        # 成长因子
        # EPS_G: 用价格变化率近似
        result['EPS_G'] = result['close'].pct_change(63)  # 季度增长
        
        # ROE_D: 用价格动量近似
        result['ROE_D'] = result['close'].pct_change(126)  # 半年变化
        
        # 质量因子
        # LEV: 用波动率倒数（低波动=高质量）
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # ROA: 用收益率稳定性近似
        result['ROA'] = result['close'].pct_change().rolling(60).mean()
        
        # GPM: 用价格相对强度
        result['GPM'] = result['close'] / result['close'].rolling(60).mean() - 1
        
        # 动量因子
        # MOM3: 3个月动量
        result['MOM3'] = result['close'].pct_change(63)
        
        # VOL_M_5_60: 成交量动量
        result['VOL_M_5_60'] = result['volume'].rolling(5).mean() / result['volume'].rolling(60).mean() - 1
        
        # 情绪因子
        # TURN: 换手率近似（成交量/价格）
        result['TURN'] = result['volume'] / result['close']
        
        # VOL_20: 20日波动率
        result['VOL_20'] = result['close'].pct_change().rolling(20).std()
        
        # 合成风格得分
        # 价值得分
        result['value_score'] = (
            result['EP'] * self.FACTOR_SYNTHESIS_WEIGHTS['value']['EP'] +
            result['BP'] * self.FACTOR_SYNTHESIS_WEIGHTS['value']['BP']
        )
        
        # 成长得分
        result['growth_score'] = (
            result['EPS_G'] * self.FACTOR_SYNTHESIS_WEIGHTS['growth']['EPS_G'] +
            result['ROE_D'] * self.FACTOR_SYNTHESIS_WEIGHTS['growth']['ROE_D']
        )
        
        # 质量得分
        result['quality_score'] = (
            result['LEV'] * self.FACTOR_SYNTHESIS_WEIGHTS['quality']['LEV'] +
            result['ROA'] * self.FACTOR_SYNTHESIS_WEIGHTS['quality']['ROA'] +
            result['GPM'] * self.FACTOR_SYNTHESIS_WEIGHTS['quality']['GPM']
        )
        
        # 动量得分
        result['momentum_score'] = (
            result['MOM3'] * self.FACTOR_SYNTHESIS_WEIGHTS['momentum']['MOM3'] +
            result['VOL_M_5_60'] * self.FACTOR_SYNTHESIS_WEIGHTS['momentum']['VOL_M_5_60']
        )
        
        # 情绪得分
        result['sentiment_score'] = (
            result['TURN'] * self.FACTOR_SYNTHESIS_WEIGHTS['sentiment']['TURN'] +
            result['VOL_20'] * self.FACTOR_SYNTHESIS_WEIGHTS['sentiment']['VOL_20']
        )
        
        return result
    
    def calculate_final_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算最终综合得分"""
        result = df.copy()
        
        # 长期策略权重合成
        result['final_score'] = (
            result['value_score'] * self.STYLE_WEIGHTS_LONG_TERM['value'] +
            result['growth_score'] * self.STYLE_WEIGHTS_LONG_TERM['growth'] +
            result['quality_score'] * self.STYLE_WEIGHTS_LONG_TERM['quality'] +
            result['momentum_score'] * self.STYLE_WEIGHTS_LONG_TERM['momentum'] +
            result['sentiment_score'] * self.STYLE_WEIGHTS_LONG_TERM['sentiment']
        )
        
        # 标准化
        result['final_score_z'] = (
            result['final_score'] - result['final_score'].rolling(252).mean()
        ) / result['final_score'].rolling(252).std()
        
        return result
    
    def generate_signal(self, df: pd.DataFrame, threshold: float = 0.5) -> pd.DataFrame:
        """生成交易信号"""
        result = df.copy()
        
        # 基于综合得分的信号
        result['signal'] = 0
        result.loc[result['final_score_z'] > threshold, 'signal'] = 1   # 买入
        result.loc[result['final_score_z'] < -threshold, 'signal'] = -1  # 卖出
        
        return result
    
    def run_backtest(self, stock_code: str) -> Dict:
        """运行单只股票回测"""
        try:
            # 获取数据
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 252:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            # 计算因子
            df = self.calculate_style_scores(df)
            df = self.calculate_final_score(df)
            df = self.generate_signal(df)
            
            # 回测计算
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['signal'].shift(1) * df['returns']
            
            # 去除NaN
            df = df.dropna()
            
            if len(df) < 100:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            # 计算绩效
            total_return = (1 + df['strategy_returns']).prod() - 1
            annual_return = (1 + total_return) ** (252 / len(df)) - 1
            volatility = df['strategy_returns'].std() * np.sqrt(252)
            sharpe = annual_return / volatility if volatility > 0 else 0
            
            # 最大回撤
            cum_returns = (1 + df['strategy_returns']).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # 基准收益（买入持有）
            benchmark_return = df['close'].iloc[-1] / df['close'].iloc[0] - 1
            
            return {
                'stock_code': stock_code,
                'status': 'success',
                'total_return': round(total_return * 100, 2),
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'benchmark_return': round(benchmark_return * 100, 2),
                'excess_return': round((total_return - benchmark_return) * 100, 2),
                'trading_days': len(df),
                'win_rate': round((df['strategy_returns'] > 0).sum() / len(df) * 100, 1)
            }
            
        except Exception as e:
            return {'stock_code': stock_code, 'status': 'error', 'error': str(e)}
    
    def run_batch_backtest(self, stock_codes: List[str] = None, max_workers: int = 4, stock_limit: int = 50) -> Dict:
        """批量回测"""
        if stock_codes is None:
            stock_codes = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*60}")
        print("方法2：因子合成回测验证（长期策略）")
        print(f"{'='*60}")
        print(f"股票数量: {len(stock_codes)}")
        print(f"风格权重: 价值30% + 质量32% + 成长20% + 动量10% + 情绪8%")
        print(f"回测周期: 3年")
        print(f"{'='*60}\n")
        
        results = []
        success_count = 0
        
        # 并行回测
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.run_backtest, code): code for code in stock_codes}
            
            for i, future in enumerate(as_completed(futures), 1):
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    success_count += 1
                
                if i % 100 == 0:
                    print(f"进度: {i}/{len(stock_codes)} ({success_count} 成功)")
        
        # 统计结果
        successful_results = [r for r in results if r['status'] == 'success']
        
        if not successful_results:
            print("没有成功的回测结果")
            return {'status': 'no_results'}
        
        df_results = pd.DataFrame(successful_results)
        
        # 汇总统计
        summary = {
            'total_stocks': len(stock_codes),
            'success_count': success_count,
            'avg_annual_return': round(df_results['annual_return'].mean(), 2),
            'median_annual_return': round(df_results['annual_return'].median(), 2),
            'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
            'median_sharpe': round(df_results['sharpe_ratio'].median(), 3),
            'avg_max_drawdown': round(df_results['max_drawdown'].mean(), 2),
            'avg_win_rate': round(df_results['win_rate'].mean(), 1),
            'avg_excess_return': round(df_results['excess_return'].mean(), 2),
            'positive_return_pct': round((df_results['annual_return'] > 0).sum() / len(df_results) * 100, 1),
            'beat_benchmark_pct': round((df_results['excess_return'] > 0).sum() / len(df_results) * 100, 1)
        }
        
        print(f"\n{'='*60}")
        print("回测结果汇总")
        print(f"{'='*60}")
        print(f"成功回测: {success_count}/{len(stock_codes)}")
        print(f"平均年化收益: {summary['avg_annual_return']}%")
        print(f"中位数年化收益: {summary['median_annual_return']}%")
        print(f"平均夏普比率: {summary['avg_sharpe']}")
        print(f"平均最大回撤: {summary['avg_max_drawdown']}%")
        print(f"平均超额收益: {summary['avg_excess_return']}%")
        print(f"正收益占比: {summary['positive_return_pct']}%")
        print(f"跑赢基准占比: {summary['beat_benchmark_pct']}%")
        
        # Top 20 股票
        top_20 = df_results.nlargest(20, 'annual_return')[['stock_code', 'annual_return', 'sharpe_ratio', 'max_drawdown', 'excess_return']]
        
        print(f"\nTop 20 表现最好:")
        print("-"*60)
        for i, row in top_20.iterrows():
            print(f"{row['stock_code']:12s} 年化{row['annual_return']:>6.1f}% 夏普{row['sharpe_ratio']:>5.2f} 回撤{row['max_drawdown']:>6.1f}%")
        
        # 保存结果
        output = {
            'method': 'factor_synthesis_long_term',
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'style_weights': self.STYLE_WEIGHTS_LONG_TERM,
            'summary': summary,
            'top_20': top_20.to_dict('records'),
            'all_results': successful_results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"method2_synthesis_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    """运行方法2回测"""
    engine = FactorSynthesisBacktest()
    result = engine.run_batch_backtest(max_workers=4)
    return result


if __name__ == "__main__":
    main()