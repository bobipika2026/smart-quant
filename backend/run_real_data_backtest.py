"""
真实财务数据因子优化

使用真实财务数据：
- daily_basic.csv: PE、PB、PS、股息率、市值、换手率
- fina_indicator.csv: ROE、ROA、毛利率、净利率、杠杆率
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from itertools import combinations
import json
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class RealDataFactorOptimizer:
    """真实财务数据因子优化器"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    FINANCIAL_DIR = "data_cache/financial"
    
    # 因子配置（基于真实数据，调整权重）
    FACTOR_CONFIG = {
        # 技术因子（从日线计算）- 权重提高
        'KDJ': {'weight': 0.25, 'category': 'technical', 'source': 'price'},
        'BOLL': {'weight': 0.20, 'category': 'technical', 'source': 'price'},
        'MOM': {'weight': 0.18, 'category': 'momentum', 'source': 'price'},
        
        # 质量因子（从fina_indicator）- 核心因子
        'ROE': {'weight': 0.15, 'category': 'quality', 'source': 'fina_indicator', 'field': 'roe'},
        
        # 情绪因子（从daily_basic）
        'TURN': {'weight': 0.12, 'category': 'sentiment', 'source': 'daily_basic', 'field': 'turnover_rate'},
        
        # 价值因子（从daily_basic）- 权重降低
        'EP': {'weight': 0.05, 'category': 'value', 'source': 'daily_basic', 'field': 'pe_ttm'},
        'BP': {'weight': 0.05, 'category': 'value', 'source': 'daily_basic', 'field': 'pb'},
    }
    
    # 仓位控制
    MAX_POSITION = 0.95
    VOL_TARGET = 0.20
    STOP_LOSS = 0.10
    TAKE_PROFIT = 0.20
    
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
            files = [f.replace('_daily_basic.csv', '') 
                    for f in os.listdir(self.FINANCIAL_DIR) 
                    if f.endswith('_daily_basic.csv')]
            return files[:limit]
    
    def load_stock_data(self, stock_code: str) -> Dict[str, pd.DataFrame]:
        """加载股票数据（日线+财务）"""
        data = {}
        
        # 日线数据
        day_files = [
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SZ_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SH_day.csv"),
        ]
        for fp in day_files:
            if os.path.exists(fp):
                df = pd.read_csv(fp, encoding='utf-8')
                column_map = {'日期': 'trade_date', '开盘': 'open', '最高': 'high', 
                              '最低': 'low', '收盘': 'close', '成交量': 'volume', '成交额': 'amount'}
                df = df.rename(columns=column_map)
                df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
                df.set_index('trade_date', inplace=True)
                data['day'] = df[['open', 'high', 'low', 'close', 'volume', 'amount']]
                break
        
        # 每日基本面数据
        basic_files = [
            os.path.join(self.FINANCIAL_DIR, f"{stock_code}_daily_basic.csv"),
            os.path.join(self.FINANCIAL_DIR, f"{stock_code}.SZ_daily_basic.csv"),
        ]
        for fp in basic_files:
            if os.path.exists(fp):
                df = pd.read_csv(fp, encoding='utf-8')
                df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d')
                df.set_index('trade_date', inplace=True)
                data['daily_basic'] = df
                break
        
        # 财务指标数据
        indicator_files = [
            os.path.join(self.FINANCIAL_DIR, f"{stock_code}_fina_indicator.csv"),
            os.path.join(self.FINANCIAL_DIR, f"{stock_code}.SZ_fina_indicator.csv"),
        ]
        for fp in indicator_files:
            if os.path.exists(fp):
                df = pd.read_csv(fp, encoding='utf-8')
                df['end_date'] = pd.to_datetime(df['end_date'].astype(str), format='%Y%m%d')
                df.set_index('end_date', inplace=True)
                data['fina_indicator'] = df
                break
        
        return data
    
    def calculate_real_factors(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """用真实数据计算因子"""
        if 'day' not in data or 'daily_basic' not in data:
            return pd.DataFrame()
        
        df = data['day'].copy()
        
        # ==================== 技术因子（从价格计算） ====================
        
        # KDJ
        low_14 = df['low'].rolling(14).min()
        high_14 = df['high'].rolling(14).max()
        rsv = (df['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        df['KDJ_K'] = rsv.ewm(alpha=1/3).mean()
        df['KDJ_D'] = df['KDJ_K'].ewm(alpha=1/3).mean()
        df['KDJ'] = df['KDJ_D']
        
        # BOLL
        ma20 = df['close'].rolling(20).mean()
        std20 = df['close'].rolling(20).std()
        df['BOLL'] = (df['close'] - ma20) / (2 * std20 + 0.0001)
        
        # MOM（3个月动量）
        df['MOM'] = df['close'].pct_change(63)
        
        # ==================== 价值因子（从daily_basic） ====================
        
        basic = data['daily_basic']
        
        # EP = 1/PE_TTM（PE越低，EP越高，投资价值越大）
        if 'pe_ttm' in basic.columns:
            pe = basic['pe_ttm'].replace([0, np.inf, -np.inf], np.nan)
            df['EP'] = 1 / pe  # PE越低，EP越高
        else:
            df['EP'] = 1 / (df['close'] + 0.0001) * 100
        
        # BP = 1/PB（PB越低，BP越高，投资价值越大）
        if 'pb' in basic.columns:
            pb = basic['pb'].replace([0, np.inf, -np.inf], np.nan)
            df['BP'] = 1 / pb  # PB越低，BP越高
        else:
            df['BP'] = 1 / (df['close'] + 0.0001) * 50
        
        # DIV_YIELD（股息率，直接使用，越高越好）
        if 'dv_ttm' in basic.columns:
            df['DIV_YIELD'] = basic['dv_ttm'].fillna(0) / 100  # 转为小数
        else:
            df['DIV_YIELD'] = 0.02
        
        # ==================== 情绪因子（从daily_basic） ====================
        
        # TURN（换手率，直接使用）
        if 'turnover_rate' in basic.columns:
            df['TURN'] = basic['turnover_rate'] / 100  # 转为小数
        else:
            df['TURN'] = df['volume'] / (df['close'] + 0.0001)
        
        # ==================== 质量因子（从fina_indicator，按日期向前填充） ====================
        
        if 'fina_indicator' in data and len(data['fina_indicator']) > 0:
            indicator = data['fina_indicator']
            
            # ROE
            if 'roe' in indicator.columns:
                roe_series = indicator['roe'].copy()
                # 将季度数据映射到日线
                df['ROE'] = np.nan
                for date, roe in roe_series.items():
                    # 财报发布后，该ROE值持续有效
                    mask = df.index >= date
                    df.loc[mask, 'ROE'] = roe
                df['ROE'] = df['ROE'].ffill()
            
            # GPM（毛利率）
            if 'grossprofit_margin' in indicator.columns:
                gpm_series = indicator['grossprofit_margin'].copy()
                df['GPM'] = np.nan
                for date, gpm in gpm_series.items():
                    mask = df.index >= date
                    df.loc[mask, 'GPM'] = gpm
                df['GPM'] = df['GPM'].ffill()
        
        # 如果财务数据缺失，用价格近似
        if 'ROE' not in df.columns or df['ROE'].isna().all():
            df['ROE'] = df['close'].pct_change().rolling(252).mean() / \
                       (df['close'].pct_change().rolling(252).std() + 0.0001)
        
        if 'GPM' not in df.columns or df['GPM'].isna().all():
            df['GPM'] = df['close'] / (df['close'].rolling(60).mean() + 0.0001) - 1
        
        return df
    
    def calculate_composite_score(self, df: pd.DataFrame, factor_combo: List[str]) -> pd.DataFrame:
        """计算综合得分"""
        result = df.copy()
        
        # 加权计算
        result['score'] = 0
        total_weight = 0
        
        for factor in factor_combo:
            if factor in self.FACTOR_CONFIG and factor in result.columns:
                weight = self.FACTOR_CONFIG[factor]['weight']
                factor_val = result[factor].fillna(0)
                
                # 某些因子需要取负（如EP、BP本身是正向的，但原值越高越好）
                # 标准化
                factor_std = (factor_val - factor_val.rolling(252).mean()) / \
                            (factor_val.rolling(252).std() + 0.0001)
                
                result['score'] += factor_std * weight
                total_weight += weight
        
        if total_weight > 0:
            result['score'] = result['score'] / total_weight
        
        # 标准化
        result['score_z'] = (result['score'] - result['score'].rolling(252).mean()) / \
                           (result['score'].rolling(252).std() + 0.0001)
        
        return result
    
    def generate_signal(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成信号"""
        result = df.copy()
        
        # 趋势过滤
        ma60 = result['close'].rolling(60).mean()
        ma120 = result['close'].rolling(120).mean()
        result['trend'] = np.where(ma60 > ma120, 1, -1)
        
        # 多因子投票
        result['vote'] = 0
        for factor in ['KDJ', 'BOLL', 'MOM', 'EP', 'BP', 'ROE']:
            if factor in result.columns:
                factor_std = (result[factor] - result[factor].rolling(252).mean()) / \
                            (result[factor].rolling(252).std() + 0.0001)
                result['vote'] += np.where(factor_std > 0, 1, np.where(factor_std < 0, -1, 0))
        
        # 综合信号
        result['signal'] = 0
        buy_cond = (result['trend'] >= 0) & (result['score_z'] > 0.3) & (result['vote'] >= 1)
        sell_cond = (result['trend'] == -1) & (result['score_z'] < -0.3) & (result['vote'] <= -1)
        
        result.loc[buy_cond, 'signal'] = 1
        result.loc[sell_cond, 'signal'] = -1
        
        return result
    
    def calculate_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算仓位"""
        result = df.copy()
        
        # 波动率仓位
        daily_vol = result['close'].pct_change().rolling(20).std() * np.sqrt(252)
        vol_position = self.VOL_TARGET / (daily_vol + 0.0001)
        vol_position = vol_position.clip(0, self.MAX_POSITION)
        
        # 信号强度仓位
        signal_strength = np.abs(result['score_z'])
        signal_position = np.where(signal_strength > 1.0, 0.8,
                                   np.where(signal_strength > 0.5, 0.6, 0.4))
        
        # 综合仓位
        result['position'] = vol_position * signal_position
        result['position'] = result['position'].clip(0, self.MAX_POSITION)
        result['position'] = result['position'] * np.abs(result['signal'])
        
        return result
    
    def run_backtest(self, stock_code: str, factor_combo: List[str]) -> Dict:
        """运行回测"""
        try:
            data = self.load_stock_data(stock_code)
            if 'day' not in data or len(data['day']) < 1500:
                return None
            
            df = self.calculate_real_factors(data)
            if df.empty or len(df) < 1500:
                return None
            
            df = self.calculate_composite_score(df, factor_combo)
            df = self.generate_signal(df)
            df = self.calculate_position(df)
            
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
            
            # 胜率
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
            
            return {
                'stock_code': stock_code,
                'factors': factor_combo,
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'excess_return': round((annual_return - benchmark_annual) * 100, 2),
                'win_rate': round(win_rate, 1),
                'short_term_return': round(short_return, 2),
                'short_term_winrate': round(short_win, 1),
                'trading_days': len(df)
            }
            
        except Exception as e:
            return None
    
    def optimize(self, stock_limit: int = 50) -> Dict:
        """优化因子组合"""
        stocks = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("真实财务数据因子优化")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"数据源: daily_basic + fina_indicator")
        print(f"{'='*70}\n")
        
        # 测试组合（侧重技术+质量）
        factor_sets = [
            ['KDJ', 'BOLL', 'MOM'],                    # 纯技术
            ['KDJ', 'BOLL', 'ROE'],                    # 技术+质量
            ['KDJ', 'BOLL', 'MOM', 'ROE'],             # 技术+动量+质量
            ['KDJ', 'BOLL', 'TURN'],                   # 技术+情绪
            ['KDJ', 'BOLL', 'MOM', 'TURN'],            # 技术+动量+情绪
            ['KDJ', 'BOLL', 'MOM', 'ROE', 'TURN'],     # 综合组合
        ]
        
        all_results = {}
        
        for i, factor_combo in enumerate(factor_sets, 1):
            combo_name = '+'.join(factor_combo)
            print(f"\n测试组合 {i}/{len(factor_sets)}: {combo_name}")
            sys.stdout.flush()
            
            results = []
            for stock in stocks:
                result = self.run_backtest(stock, factor_combo)
                if result:
                    results.append(result)
            
            if results:
                df_results = pd.DataFrame(results)
                
                all_results[combo_name] = {
                    'factors': factor_combo,
                    'success_count': len(results),
                    'avg_return': round(df_results['annual_return'].mean(), 2),
                    'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
                    'avg_drawdown': round(df_results['max_drawdown'].mean(), 2),
                    'avg_winrate': round(df_results['win_rate'].mean(), 1),
                    'avg_short_return': round(df_results['short_term_return'].mean(), 2),
                    'avg_short_winrate': round(df_results['short_term_winrate'].mean(), 1),
                    'beat_benchmark_pct': round((df_results['excess_return'] > 0).sum() / len(df_results) * 100, 1)
                }
                
                print(f"  长期年化: {all_results[combo_name]['avg_return']}%")
                print(f"  夏普比率: {all_results[combo_name]['avg_sharpe']}")
                print(f"  短期收益: {all_results[combo_name]['avg_short_return']}%")
                print(f"  短期胜率: {all_results[combo_name]['avg_short_winrate']}%")
                sys.stdout.flush()
        
        # 结果对比
        print(f"\n{'='*70}")
        print("优化结果对比")
        print(f"{'='*70}")
        print(f"\n{'组合':<35s} {'长期收益':>10s} {'夏普':>8s} {'短期收益':>10s} {'短期胜率':>10s}")
        print("-" * 75)
        
        best_combo = None
        best_score = -999
        
        for combo_name, data in all_results.items():
            score = data['avg_sharpe'] * 10 + data['avg_short_winrate'] / 10
            if score > best_score:
                best_score = score
                best_combo = combo_name
            
            print(f"{combo_name:<35s} {data['avg_return']:>9.2f}% {data['avg_sharpe']:>8.3f} "
                  f"{data['avg_short_return']:>9.2f}% {data['avg_short_winrate']:>9.1f}%")
        
        if best_combo:
            best_data = all_results[best_combo]
            print(f"\n{'='*70}")
            print("🏆 最优因子组合（真实财务数据）")
            print(f"{'='*70}")
            print(f"组合: {best_combo}")
            print(f"因子: {', '.join(best_data['factors'])}")
            print(f"\n长期验证:")
            print(f"  年化收益: {best_data['avg_return']}%")
            print(f"  夏普比率: {best_data['avg_sharpe']}")
            print(f"  最大回撤: {best_data['avg_drawdown']}%")
            print(f"  跑赢基准: {best_data['beat_benchmark_pct']}%")
            print(f"\n短期验证:")
            print(f"  短期收益: {best_data['avg_short_return']}%")
            print(f"  短期胜率: {best_data['avg_short_winrate']}%")
        
        # 保存
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_source': 'real_financial_data',
            'best_combo': best_combo,
            'best_data': best_data if best_combo else None,
            'all_results': all_results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"real_data_factor_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = RealDataFactorOptimizer()
    return optimizer.optimize(stock_limit=50)


if __name__ == "__main__":
    main()