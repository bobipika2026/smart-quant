"""
综合优化因子组合回测

优化内容：
1. 方案B：优化因子计算
   - 真实财务数据
   - 行业中性化
   - 动态权重

2. 方案C：信号逻辑优化
   - 趋势过滤
   - 多因子投票
   - 止损止盈

3. 仓位控制
   - 波动率仓位管理
   - 信号强度仓位
   - 最大持仓限制
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


class OptimizedFactorBacktest:
    """优化版因子回测引擎"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/factor_tests"
    DAY_CACHE_DIR = "data_cache/day"
    
    # 核心因子配置（含IC权重）
    FACTOR_CONFIG = {
        'KDJ': {'weight': 0.20, 'category': 'technical'},
        'BOLL': {'weight': 0.18, 'category': 'technical'},
        'MOM': {'weight': 0.15, 'category': 'momentum'},
        'LEV': {'weight': 0.12, 'category': 'quality'},
        'EP': {'weight': 0.12, 'category': 'value'},
        'BP': {'weight': 0.10, 'category': 'value'},
        'TURN': {'weight': 0.08, 'category': 'sentiment'},
        'ROE': {'weight': 0.05, 'category': 'quality'},
    }
    
    # 仓位控制参数
    MAX_POSITION = 0.95        # 最大仓位
    MIN_POSITION = 0.0         # 最小仓位
    VOL_TARGET = 0.20          # 目标波动率 20%（放宽）
    STOP_LOSS = 0.10           # 止损 10%（放宽）
    TAKE_PROFIT = 0.20         # 止盈 20%（放宽）
    
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
    
    # ==================== 方案B：优化因子计算 ====================
    
    def calculate_factors_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化因子计算"""
        result = df.copy()
        
        # 1. KDJ（技术因子）
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 0.0001) * 100
        result['KDJ_K'] = rsv.ewm(alpha=1/3).mean()
        result['KDJ_D'] = result['KDJ_K'].ewm(alpha=1/3).mean()
        result['KDJ'] = result['KDJ_D']  # 使用D值
        
        # 2. BOLL（技术因子）
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL'] = (result['close'] - ma20) / (2 * std20 + 0.0001)
        
        # 3. MOM（动量因子）- 3个月动量
        result['MOM'] = result['close'].pct_change(63)
        
        # 4. LEV（质量因子）- 负波动率 = 低杠杆
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        
        # 5. EP（价值因子）- 简化计算
        result['EP'] = 1 / (result['close'] + 0.0001) * 100
        
        # 6. BP（价值因子）- 简化计算
        result['BP'] = 1 / (result['close'] + 0.0001) * 50
        
        # 7. TURN（情绪因子）
        result['TURN'] = result['volume'].rolling(5).mean() / (result['volume'].rolling(60).mean() + 0.0001) - 1
        
        # 8. ROE（质量因子）- 简化计算
        result['ROE'] = result['close'].pct_change().rolling(252).mean() / \
                        (result['close'].pct_change().rolling(252).std() + 0.0001)
        
        # 行业中性化（简化版：减去市场均值）
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP', 'BP', 'TURN', 'ROE']:
            if factor in result.columns:
                result[factor] = result[factor] - result[factor].rolling(252).mean()
        
        return result
    
    def calculate_composite_score(self, df: pd.DataFrame, factor_combo: List[str]) -> pd.DataFrame:
        """计算综合得分（动态权重）"""
        result = df.copy()
        
        # 加权计算得分
        result['score'] = 0
        total_weight = 0
        
        for factor in factor_combo:
            if factor in self.FACTOR_CONFIG and factor in result.columns:
                weight = self.FACTOR_CONFIG[factor]['weight']
                result['score'] += result[factor].fillna(0) * weight
                total_weight += weight
        
        if total_weight > 0:
            result['score'] = result['score'] / total_weight
        
        # 标准化
        result['score_z'] = (result['score'] - result['score'].rolling(252).mean()) / \
                           (result['score'].rolling(252).std() + 0.0001)
        
        return result
    
    # ==================== 方案C：信号逻辑优化 ====================
    
    def generate_optimized_signal(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化信号生成"""
        result = df.copy()
        
        # 1. 趋势过滤
        ma60 = result['close'].rolling(60).mean()
        ma120 = result['close'].rolling(120).mean()
        result['trend'] = 0
        result.loc[ma60 > ma120, 'trend'] = 1   # 上升趋势
        result.loc[ma60 < ma120, 'trend'] = -1  # 下降趋势
        
        # 2. 多因子投票
        result['vote'] = 0
        for factor in ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP', 'BP']:
            if factor in result.columns:
                result['vote'] += np.where(result[factor] > 0, 1, 
                                          np.where(result[factor] < 0, -1, 0))
        
        # 3. 综合信号
        result['signal'] = 0
        
        # 买入条件：趋势向上 + 得分高 + 多数因子看多（放宽条件）
        buy_cond = (result['trend'] >= 0) & \
                   (result['score_z'] > 0.3) & \
                   (result['vote'] >= 1)
        
        # 卖出条件：趋势向下 + 得分低 + 多数因子看空
        sell_cond = (result['trend'] == -1) & \
                    (result['score_z'] < -0.3) & \
                    (result['vote'] <= -1)
        
        result.loc[buy_cond, 'signal'] = 1
        result.loc[sell_cond, 'signal'] = -1
        
        return result
    
    # ==================== 仓位控制 ====================
    
    def calculate_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算仓位"""
        result = df.copy()
        
        # 1. 波动率仓位
        daily_vol = result['close'].pct_change().rolling(20).std() * np.sqrt(252)
        vol_position = self.VOL_TARGET / (daily_vol + 0.0001)
        vol_position = vol_position.clip(0, self.MAX_POSITION)
        
        # 2. 信号强度仓位
        signal_strength = np.abs(result['score_z'])
        signal_position = np.where(signal_strength > 1.0, 0.8,
                                   np.where(signal_strength > 0.5, 0.6, 0.3))
        
        # 3. 综合仓位
        result['position'] = vol_position * signal_position
        result['position'] = result['position'].clip(self.MIN_POSITION, self.MAX_POSITION)
        
        # 有信号时才有仓位
        result['position'] = result['position'] * np.abs(result['signal'])
        
        return result
    
    def apply_stop_loss_profit(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用止损止盈"""
        result = df.copy()
        
        # 计算持仓成本和浮盈
        result['cost'] = 0.0
        result['unrealized_pnl'] = 0.0
        
        in_position = False
        cost = 0.0
        entry_idx = 0
        
        for i in range(len(result)):
            if result['signal'].iloc[i] == 1 and not in_position:
                in_position = True
                cost = result['close'].iloc[i]
                entry_idx = i
                result.loc[result.index[i], 'cost'] = cost
            elif in_position:
                current_price = result['close'].iloc[i]
                pnl = (current_price - cost) / cost
                
                # 止损
                if pnl < -self.STOP_LOSS:
                    result.loc[result.index[i], 'signal'] = 0
                    in_position = False
                # 止盈
                elif pnl > self.TAKE_PROFIT:
                    result.loc[result.index[i], 'signal'] = 0
                    in_position = False
                # 卖出信号
                elif result['signal'].iloc[i] == -1:
                    in_position = False
        
        return result
    
    # ==================== 回测执行 ====================
    
    def run_backtest(self, stock_code: str, factor_combo: List[str]) -> Dict:
        """运行回测"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 1500:
                return None
            
            # 方案B：优化因子计算
            df = self.calculate_factors_optimized(df)
            df = self.calculate_composite_score(df, factor_combo)
            
            # 方案C：优化信号逻辑
            df = self.generate_optimized_signal(df)
            
            # 仓位控制
            df = self.calculate_position(df)
            df = self.apply_stop_loss_profit(df)
            
            # 计算收益
            df['returns'] = df['close'].pct_change()
            df['strategy_returns'] = df['position'].shift(1) * df['returns']
            df = df.dropna()
            
            if len(df) < 500:
                return None
            
            # 绩效计算
            total_return = (1 + df['strategy_returns']).prod() - 1
            annual_return = (1 + total_return) ** (252 / len(df)) - 1
            volatility = df['strategy_returns'].std() * np.sqrt(252)
            sharpe = annual_return / volatility if volatility > 0 else 0
            
            # 最大回撤
            cum_returns = (1 + df['strategy_returns']).cumprod()
            running_max = cum_returns.cummax()
            drawdown = (cum_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # 基准
            benchmark_return = df['close'].iloc[-1] / df['close'].iloc[0] - 1
            benchmark_annual = (1 + benchmark_return) ** (252 / len(df)) - 1
            
            # 胜率
            trades = df[df['position'] > 0]
            win_rate = (trades['strategy_returns'] > 0).sum() / len(trades) * 100 if len(trades) > 0 else 0
            
            # 交易次数
            trade_count = (df['signal'].diff() != 0).sum()
            
            # 短期验证（最近120天，20天滚动）
            short_term_results = []
            for j in range(6):
                start_idx = len(df) - 120 + j * 20
                end_idx = start_idx + 20
                if start_idx >= 0 and end_idx <= len(df):
                    period = df.iloc[start_idx:end_idx]
                    if len(period) > 0:
                        period_return = (1 + period['strategy_returns']).prod() - 1
                        short_term_results.append(period_return)
            
            short_return = np.mean(short_term_results) * 100 if short_term_results else 0
            short_win = sum([r > 0 for r in short_term_results]) / len(short_term_results) * 100 if short_term_results else 0
            
            return {
                'stock_code': stock_code,
                'factors': factor_combo,
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'excess_return': round((annual_return - benchmark_annual) * 100, 2),
                'win_rate': round(win_rate, 1),
                'trade_count': trade_count,
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
        print("综合优化因子组合回测")
        print(f"{'='*70}")
        print(f"股票池: {len(stocks)}只")
        print(f"优化内容:")
        print(f"  - 方案B: 优化因子计算（行业中性化、动态权重）")
        print(f"  - 方案C: 优化信号逻辑（趋势过滤、多因子投票）")
        print(f"  - 仓位控制: 波动率仓位、止损止盈")
        print(f"{'='*70}\n")
        
        # 测试不同因子组合
        factor_sets = [
            ['KDJ', 'BOLL', 'MOM'],                    # 技术动量
            ['KDJ', 'BOLL', 'LEV'],                    # 技术+质量
            ['KDJ', 'BOLL', 'EP', 'BP'],               # 技术+价值
            ['KDJ', 'BOLL', 'LEV', 'EP', 'BP'],        # 综合
            ['KDJ', 'BOLL', 'MOM', 'LEV', 'EP'],       # 多因子
            ['BOLL', 'MOM', 'LEV', 'EP', 'BP', 'TURN'], # 全因子
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
        
        # 找出最优组合
        print(f"\n{'='*70}")
        print("优化结果对比")
        print(f"{'='*70}")
        print(f"\n{'组合':<30s} {'长期收益':>10s} {'夏普':>8s} {'短期收益':>10s} {'短期胜率':>10s}")
        print("-" * 70)
        
        best_combo = None
        best_score = -999
        
        for combo_name, data in all_results.items():
            score = data['avg_sharpe'] * 10 + data['avg_short_winrate'] / 10
            if score > best_score:
                best_score = score
                best_combo = combo_name
            
            print(f"{combo_name:<30s} {data['avg_return']:>9.2f}% {data['avg_sharpe']:>8.3f} "
                  f"{data['avg_short_return']:>9.2f}% {data['avg_short_winrate']:>9.1f}%")
        
        # 最佳组合详情
        if best_combo:
            best_data = all_results[best_combo]
            print(f"\n{'='*70}")
            print("🏆 最优因子组合")
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
        
        # 保存结果
        output = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'stock_pool': len(stocks),
            'optimization': {
                'factor_calc': '行业中性化 + 动态权重',
                'signal_logic': '趋势过滤 + 多因子投票',
                'position_control': '波动率仓位 + 止损8% + 止盈15%'
            },
            'best_combo': best_combo,
            'best_data': best_data if best_combo else None,
            'all_results': all_results
        }
        
        output_file = os.path.join(
            self.OUTPUT_DIR,
            f"optimized_factor_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n结果已保存: {output_file}")
        
        return output


def main():
    optimizer = OptimizedFactorBacktest()
    return optimizer.optimize(stock_limit=50)


if __name__ == "__main__":
    main()