"""
综合优化框架：市场择时 + 因子动态 + 动态仓位

针对50只股票池优化
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class ComprehensiveOptimizer:
    """综合优化器：择时 + 动态因子 + 动态仓位"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    FINANCIAL_DIR = "data_cache/financial"
    
    # 因子库（按市场环境分类）
    FACTORS_BY_REGIME = {
        'bull': {  # 牛市：动量、成长
            'KDJ': 0.25,
            'BOLL': 0.20,
            'MOM': 0.25,
            'TURN': 0.15,
            'ROE': 0.15,
        },
        'bear': {  # 熊市：质量、价值
            'KDJ': 0.15,
            'BOLL': 0.15,
            'LEV': 0.20,
            'ROE': 0.25,
            'BP': 0.15,
            'EP': 0.10,
        },
        'sideways': {  # 震荡市：情绪、技术
            'KDJ': 0.25,
            'BOLL': 0.25,
            'TURN': 0.20,
            'MOM': 0.15,
            'ROE': 0.15,
        }
    }
    
    # 仓位控制参数
    BASE_POSITION = 0.6         # 基础仓位
    MAX_POSITION = 0.95         # 最大仓位
    MIN_POSITION = 0.0          # 最小仓位（空仓）
    
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
    
    def get_index_data(self) -> pd.DataFrame:
        """获取指数数据（用于市场择时）"""
        # 使用沪深300或上证指数作为基准
        index_files = [
            os.path.join(self.DAY_CACHE_DIR, "000300_day.csv"),  # 沪深300
            os.path.join(self.DAY_CACHE_DIR, "000001_day.csv"),  # 平安银行作为替代
        ]
        
        for fp in index_files:
            if os.path.exists(fp):
                df = pd.read_csv(fp, encoding='utf-8')
                column_map = {'日期': 'trade_date', '收盘': 'close'}
                df = df.rename(columns=column_map)
                if 'trade_date' in df.columns:
                    df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
                    df.set_index('trade_date', inplace=True)
                    return df[['close']]
        
        return pd.DataFrame()
    
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
    
    # ==================== 市场择时 ====================
    
    def detect_market_regime(self, index_data: pd.DataFrame, window: int = 60) -> pd.DataFrame:
        """识别市场环境"""
        result = index_data.copy()
        
        # 计算收益率
        result['returns'] = result['close'].pct_change()
        
        # 趋势指标
        result['ma_short'] = result['close'].rolling(20).mean()
        result['ma_long'] = result['close'].rolling(60).mean()
        result['trend'] = (result['ma_short'] / result['ma_long'] - 1) * 100
        
        # 波动率
        result['volatility'] = result['returns'].rolling(window).std() * np.sqrt(252) * 100
        
        # 累计收益
        result['cum_return_60'] = result['close'].pct_change(60) * 100
        
        # 市场环境判断
        result['regime'] = 'sideways'  # 默认震荡
        
        # 牛市：趋势向上 + 低波动 + 正收益
        bull_cond = (result['trend'] > 2) & (result['cum_return_60'] > 5)
        result.loc[bull_cond, 'regime'] = 'bull'
        
        # 熊市：趋势向下 + 负收益
        bear_cond = (result['trend'] < -2) & (result['cum_return_60'] < -5)
        result.loc[bear_cond, 'regime'] = 'bear'
        
        return result
    
    def get_market_position_adjustment(self, regime: str) -> float:
        """根据市场环境调整仓位"""
        adjustments = {
            'bull': 1.2,      # 牛市加仓
            'bear': 0.3,      # 熊市减仓
            'sideways': 0.8,  # 震荡市适中
        }
        return adjustments.get(regime, 1.0)
    
    # ==================== 因子计算 ====================
    
    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算所有因子"""
        result = df.copy()
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ_K'] = rsv.ewm(alpha=1/3).mean()
        result['KDJ_D'] = result['KDJ_K'].ewm(alpha=1/3).mean()
        result['KDJ'] = result['KDJ_D']
        
        # BOLL
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # MOM
        result['MOM'] = result['close'].pct_change(63)
        
        # LEV (负波动率)
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # ROE (简化)
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                       (result['close'].pct_change().rolling(252).std() + 0.0001)
        
        # TURN
        result['TURN'] = result['volume'].rolling(5).mean() / \
                        (result['volume'].rolling(60).mean() + 0.0001) - 1
        
        # EP, BP (简化)
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        return result
    
    def calculate_dynamic_score(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """动态因子得分（根据市场环境调整权重）"""
        result = df.copy()
        
        factor_weights = self.FACTORS_BY_REGIME.get(regime, self.FACTORS_BY_REGIME['sideways'])
        
        result['score'] = 0
        total_weight = 0
        
        for factor, weight in factor_weights.items():
            if factor in result.columns:
                factor_std = (result[factor] - result[factor].rolling(252).mean()) / \
                            (result[factor].rolling(252).std() + 0.0001)
                result['score'] += factor_std.fillna(0) * weight
                total_weight += weight
        
        if total_weight > 0:
            result['score'] = result['score'] / total_weight
        
        result['score_z'] = (result['score'] - result['score'].rolling(252).mean()) / \
                           (result['score'].rolling(252).std() + 0.0001)
        
        return result
    
    # ==================== 动态仓位 ====================
    
    def calculate_dynamic_position(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """动态仓位计算"""
        result = df.copy()
        
        # 1. 基础仓位
        base_pos = self.BASE_POSITION
        
        # 2. 市场环境调整
        market_adj = self.get_market_position_adjustment(regime)
        
        # 3. 波动率调整
        daily_vol = result['close'].pct_change().rolling(20).std() * np.sqrt(252)
        vol_target = 0.20
        vol_adj = vol_target / (daily_vol + 0.0001)
        vol_adj = vol_adj.clip(0.5, 1.5)
        
        # 4. 信号强度调整
        signal_strength = np.abs(result['score_z'])
        signal_adj = np.where(signal_strength > 1.0, 1.2,
                             np.where(signal_strength > 0.5, 1.0, 0.8))
        
        # 5. 综合仓位
        result['position'] = base_pos * market_adj * vol_adj * signal_adj
        result['position'] = result['position'].clip(self.MIN_POSITION, self.MAX_POSITION)
        
        # 信号为0时空仓
        result.loc[result['score_z'].abs() < 0.3, 'position'] = 0
        
        return result
    
    # ==================== 回测 ====================
    
    def run_backtest(self, stock_code: str, market_df: pd.DataFrame) -> Dict:
        """综合回测"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 1500:
                return None
            
            # 计算因子
            df = self.calculate_factors(df)
            
            # 动态因子得分（根据市场环境）
            df = df.join(market_df[['regime']], how='left')
            df['regime'] = df['regime'].fillna('sideways')
            
            # 按日期计算动态得分
            df['score'] = 0
            df['score_z'] = 0
            
            for date in df.index:
                regime = df.loc[date, 'regime']
                # 使用前一天的因子值计算得分
                if date > df.index[0]:
                    prev_date = df.index[df.index.get_loc(date) - 1]
                    factor_weights = self.FACTORS_BY_REGIME.get(regime, self.FACTORS_BY_REGIME['sideways'])
                    
                    score = 0
                    total_weight = 0
                    for factor, weight in factor_weights.items():
                        if factor in df.columns:
                            factor_val = df.loc[prev_date, factor]
                            if pd.notna(factor_val):
                                # 使用滚动标准化
                                roll_mean = df.loc[:prev_date, factor].rolling(252).mean().iloc[-1]
                                roll_std = df.loc[:prev_date, factor].rolling(252).std().iloc[-1]
                                if pd.notna(roll_mean) and pd.notna(roll_std) and roll_std > 0:
                                    score += ((factor_val - roll_mean) / roll_std) * weight
                                    total_weight += weight
                    
                    if total_weight > 0:
                        df.loc[date, 'score'] = score / total_weight
            
            # 计算score_z
            df['score_z'] = (df['score'] - df['score'].rolling(252).mean()) / \
                           (df['score'].rolling(252).std() + 0.0001)
            
            # 动态仓位
            df = self.calculate_dynamic_position(df, 'sideways')  # 简化处理
            
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
            regime_dist = df['regime'].value_counts().to_dict() if 'regime' in df.columns else {}
            
            return {
                'stock_code': stock_code,
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
        """综合优化"""
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("综合优化：市场择时 + 因子动态 + 动态仓位")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"优化内容:")
        print(f"  - 市场择时：识别牛/熊/震荡，动态调整仓位")
        print(f"  - 因子动态：根据市场环境切换因子权重")
        print(f"  - 动态仓位：波动率 + 信号强度 + 市场环境")
        print(f"{'='*70}\n")
        
        # 获取市场数据
        market_df = self.get_index_data()
        if market_df.empty:
            # 使用第一只股票作为市场代理
            first_stock = self.get_stock_data(stocks[0])
            market_df = first_stock[['close']].copy()
        
        market_df = self.detect_market_regime(market_df)
        
        print("市场环境识别完成")
        regime_summary = market_df['regime'].value_counts()
        print(f"  牛市: {regime_summary.get('bull', 0)}天")
        print(f"  熊市: {regime_summary.get('bear', 0)}天")
        print(f"  震荡: {regime_summary.get('sideways', 0)}天")
        print()
        
        # 回测
        results = []
        for i, stock in enumerate(stocks, 1):
            if i % 10 == 0:
                print(f"进度: {i}/{len(stocks)}")
                sys.stdout.flush()
            
            result = self.run_backtest(stock, market_df)
            if result:
                results.append(result)
        
        if not results:
            print("无有效结果")
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
        print("综合优化结果")
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
            ['stock_code', 'annual_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']
        ]
        
        print(f"\nTop 10 表现最好:")
        print("-" * 60)
        for _, row in top_10.iterrows():
            print(f"{row['stock_code']:12s} 年化{row['annual_return']:>6.2f}% 夏普{row['sharpe_ratio']:>5.2f} "
                  f"回撤{row['max_drawdown']:>6.1f}% 胜率{row['win_rate']:>5.1f}%")
        
        # 保存
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'optimization': {
                'market_timing': '趋势+波动率识别牛/熊/震荡',
                'dynamic_factors': '按市场环境切换因子权重',
                'dynamic_position': '基础仓位×市场调整×波动率调整×信号强度'
            },
            'factor_weights_by_regime': self.FACTORS_BY_REGIME,
            'summary': summary,
            'top_10': top_10.to_dict('records'),
            'all_results': results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"comprehensive_optimization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = ComprehensiveOptimizer()
    return optimizer.optimize(stock_limit=50)


if __name__ == "__main__":
    main()