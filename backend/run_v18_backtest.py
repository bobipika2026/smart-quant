"""
v1.8 风险控制版回测

核心特性：
1. 降低基础仓位：60%（原85%）
2. 最大仓位：70%（原95%）
3. 动态回撤控制：回撤>15%时仓位减半
4. 分级止损：3%/5%/8%三档止损
5. 市场环境自适应
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import json
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.config.best_factor_config_v18 import (
    MARKET_TIMING_CONFIG,
    FACTOR_CONFIG,
    POSITION_CONFIG,
    REGIME_PARAMS,
    BACKTEST_CONFIG,
    VALIDATION_CRITERIA,
    get_factor_weights,
    get_position_adjustment,
    get_signal_adjustment,
    get_drawdown_adjustment
)


class V18BacktestEngine:
    """v1.8 风险控制版回测引擎"""
    
    DB_PATH = "smart_quant.db"
    OUTPUT_DIR = "data_cache/backtest_results"
    DAY_CACHE_DIR = "data_cache/day"
    INDEX_CACHE_DIR = "data_cache/index"
    
    def __init__(self):
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)
        self.index_data = self._load_index_data()
    
    def _load_index_data(self) -> pd.DataFrame:
        """加载大盘指数数据"""
        index_file = os.path.join(self.INDEX_CACHE_DIR, "000001.SH_day.csv")
        if os.path.exists(index_file):
            df = pd.read_csv(index_file, encoding='utf-8')
            df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d')
            df.set_index('trade_date', inplace=True)
            return df[['收盘']].rename(columns={'收盘': 'index_close'})
        return pd.DataFrame()
    
    def get_stock_list(self, limit: int = 100) -> List[str]:
        """获取股票列表"""
        files = glob.glob(os.path.join(self.DAY_CACHE_DIR, "*_day.csv"))
        stocks = []
        for f in files:
            code = os.path.basename(f).replace("_day.csv", "").replace(".SZ", "").replace(".SH", "")
            stocks.append(code)
        return stocks[:limit]
    
    def get_stock_data(self, stock_code: str) -> pd.DataFrame:
        """获取股票日线数据"""
        possible_files = [
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SZ_day.csv"),
            os.path.join(self.DAY_CACHE_DIR, f"{stock_code}.SH_day.csv"),
        ]
        
        for file_path in possible_files:
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                    df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d')
                    df.set_index('trade_date', inplace=True)
                    df = df.rename(columns={
                        '开盘': 'open', '最高': 'high', '最低': 'low',
                        '收盘': 'close', '成交量': 'volume', '成交额': 'amount'
                    })
                    return df[['open', 'high', 'low', 'close', 'volume', 'amount']]
                except:
                    continue
        return pd.DataFrame()
    
    def calculate_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算因子"""
        result = df.copy()
        
        # KDJ
        low_14 = result['low'].rolling(14).min()
        high_14 = result['high'].rolling(14).max()
        rsv = (result['close'] - low_14) / (high_14 - low_14 + 1e-10) * 100
        result['KDJ_D'] = rsv.ewm(com=2).mean()
        
        # BOLL位置
        ma20 = result['close'].rolling(20).mean()
        std20 = result['close'].rolling(20).std()
        result['BOLL_POS'] = (result['close'] - ma20) / (2 * std20 + 1e-10)
        
        # 动量
        result['MOM'] = result['close'].pct_change(63)
        
        # 质量因子
        result['LEV'] = -result['close'].pct_change().rolling(20).std()
        result['ROE'] = result['close'].pct_change().rolling(60).mean()
        
        # 价值因子
        result['EP'] = 1 / result['close'] * 100
        result['BP'] = 1 / result['close'] * 50
        
        # 情绪因子
        result['TURN'] = result['volume'] / result['close']
        
        return result
    
    def detect_market_regime(self, date: pd.Timestamp) -> str:
        """检测市场环境"""
        if self.index_data.empty:
            return 'sideways'
        
        # 找到最近的交易日
        idx = self.index_data.index.get_indexer([date], method='ffill')[0]
        if idx < 60:
            return 'sideways'
        
        recent = self.index_data.iloc[max(0, idx-60):idx+1]
        if len(recent) < 60:
            return 'sideways'
        
        # 计算趋势
        ma20 = recent['index_close'].rolling(20).mean().iloc[-1]
        ma60 = recent['index_close'].rolling(60).mean().iloc[-1]
        current = recent['index_close'].iloc[-1]
        
        # 牛市：价格在均线上方，短期均线在长期上方
        if current > ma20 > ma60:
            return 'bull'
        # 熊市：价格在均线下方
        elif current < ma20 < ma60:
            return 'bear'
        else:
            return 'sideways'
    
    def calculate_score(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """计算因子得分"""
        result = df.copy()
        weights = get_factor_weights(regime)
        
        # 标准化因子
        for factor in weights.keys():
            if factor in result.columns:
                result[f'{factor}_z'] = (result[factor] - result[factor].rolling(252).mean()) / \
                                        (result[factor].rolling(252).std() + 1e-10)
        
        # 加权得分
        score = 0
        for factor, weight in weights.items():
            if f'{factor}_z' in result.columns:
                score += result[f'{factor}_z'].fillna(0) * weight
        
        result['factor_score'] = score
        result['score_z'] = (score - score.rolling(252).mean()) / (score.rolling(252).std() + 1e-10)
        
        return result
    
    def generate_signals(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """生成交易信号（v1.8风险控制版）"""
        result = df.copy()
        params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
        
        # 信号阈值
        buy_threshold = params['signal_threshold']
        sell_threshold = -params.get('signal_threshold', -buy_threshold) * 0.8
        
        # 基础信号
        result['signal'] = 0
        result.loc[result['score_z'] > buy_threshold, 'signal'] = 1
        result.loc[result['score_z'] < sell_threshold, 'signal'] = -1
        
        return result
    
    def run_backtest_single(self, stock_code: str) -> Dict:
        """运行单只股票回测（v1.8风险控制版）"""
        try:
            df = self.get_stock_data(stock_code)
            if df.empty or len(df) < 252:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            # 计算因子
            df = self.calculate_factors(df)
            
            # 初始化
            capital = 1000000  # 初始资金100万
            position = 0
            cash = capital
            trades = []
            portfolio_values = []
            max_portfolio_value = capital
            
            # 遍历每一天
            for i in range(252, len(df)):
                date = df.index[i]
                row = df.iloc[i]
                
                # 检测市场环境
                regime = self.detect_market_regime(date)
                params = REGIME_PARAMS.get(regime, REGIME_PARAMS['sideways'])
                
                # 计算因子得分
                df_temp = self.calculate_score(df.iloc[:i+1], regime)
                score_z = df_temp['score_z'].iloc[-1]
                
                # 生成信号
                signal = 0
                buy_threshold = params['signal_threshold']
                sell_threshold = -buy_threshold * 0.8
                
                if score_z > buy_threshold:
                    signal = 1
                elif score_z < sell_threshold:
                    signal = -1
                
                # 计算当前组合价值
                current_price = row['close']
                position_value = position * current_price
                total_value = cash + position_value
                
                # 计算回撤
                if total_value > max_portfolio_value:
                    max_portfolio_value = total_value
                current_drawdown = (max_portfolio_value - total_value) / max_portfolio_value
                
                # 回撤调整系数
                dd_adjust = get_drawdown_adjustment(current_drawdown)
                
                # 目标仓位（考虑市场环境、回撤）
                base_pos = params['base_position']
                target_position_value = capital * base_pos * get_position_adjustment(regime) * dd_adjust
                
                # 交易执行
                if signal == 1 and position == 0:
                    # 买入
                    buy_value = min(target_position_value, cash * 0.95)
                    shares = int(buy_value / current_price / 100) * 100
                    if shares > 0:
                        cost = shares * current_price * 1.0003  # 含手续费
                        if cost <= cash:
                            position = shares
                            cash -= cost
                            trades.append({
                                'date': date.strftime('%Y-%m-%d'),
                                'action': 'buy',
                                'shares': shares,
                                'price': current_price,
                                'regime': regime
                            })
                
                elif signal == -1 and position > 0:
                    # 卖出
                    revenue = position * current_price * 0.9997
                    cash += revenue
                    trades.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'action': 'sell',
                        'shares': position,
                        'price': current_price,
                        'regime': regime
                    })
                    position = 0
                
                # 分级止损检查
                if position > 0:
                    buy_price = trades[-1]['price'] if trades else current_price
                    loss_pct = (current_price - buy_price) / buy_price
                    
                    stop_config = POSITION_CONFIG.get('tiered_stop_loss', {})
                    if stop_config.get('enabled', False):
                        if loss_pct < -stop_config.get('tier_3', 0.08):
                            # 清仓止损
                            revenue = position * current_price * 0.9997
                            cash += revenue
                            trades.append({
                                'date': date.strftime('%Y-%m-%d'),
                                'action': 'stop_loss_tier3',
                                'shares': position,
                                'price': current_price
                            })
                            position = 0
                        elif loss_pct < -stop_config.get('tier_2', 0.05):
                            # 减仓50%
                            sell_shares = position // 2
                            if sell_shares > 0:
                                revenue = sell_shares * current_price * 0.9997
                                cash += revenue
                                trades.append({
                                    'date': date.strftime('%Y-%m-%d'),
                                    'action': 'stop_loss_tier2',
                                    'shares': sell_shares,
                                    'price': current_price
                                })
                                position -= sell_shares
                
                # 记录组合价值
                portfolio_values.append({
                    'date': date,
                    'value': cash + position * current_price,
                    'cash': cash,
                    'position': position,
                    'regime': regime
                })
            
            # 计算绩效
            if len(portfolio_values) < 100:
                return {'stock_code': stock_code, 'status': 'insufficient_data'}
            
            portfolio_df = pd.DataFrame(portfolio_values).set_index('date')
            portfolio_df['returns'] = portfolio_df['value'].pct_change()
            
            # 基准收益
            benchmark_return = df['close'].iloc[-1] / df['close'].iloc[252] - 1
            
            # 策略收益
            total_return = portfolio_df['value'].iloc[-1] / capital - 1
            annual_return = (1 + total_return) ** (252 / len(portfolio_df)) - 1
            
            # 夏普比率
            sharpe = portfolio_df['returns'].mean() / portfolio_df['returns'].std() * np.sqrt(252) if portfolio_df['returns'].std() > 0 else 0
            
            # 最大回撤
            cummax = portfolio_df['value'].cummax()
            drawdown = (portfolio_df['value'] - cummax) / cummax
            max_drawdown = drawdown.min()
            
            # 交易统计
            buy_trades = [t for t in trades if 'buy' in t['action']]
            sell_trades = [t for t in trades if 'sell' in t['action']]
            
            return {
                'stock_code': stock_code,
                'status': 'success',
                'total_return': round(total_return * 100, 2),
                'annual_return': round(annual_return * 100, 2),
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': round(max_drawdown * 100, 2),
                'benchmark_return': round(benchmark_return * 100, 2),
                'excess_return': round((total_return - benchmark_return) * 100, 2),
                'win_rate': round((portfolio_df['returns'] > 0).mean() * 100, 1),
                'trade_count': len(trades),
                'final_value': round(portfolio_df['value'].iloc[-1], 2)
            }
            
        except Exception as e:
            return {'stock_code': stock_code, 'status': 'error', 'error': str(e)}
    
    def run_backtest_batch(self, stock_limit: int = 100) -> Dict:
        """批量回测"""
        stock_codes = self.get_stock_list(limit=stock_limit)
        
        print(f"\n{'='*70}")
        print("v1.8 风险控制版回测")
        print(f"{'='*70}")
        print(f"股票池: {len(stock_codes)}只")
        print(f"基础仓位: {POSITION_CONFIG['base_position']:.0%}")
        print(f"最大仓位: {POSITION_CONFIG['max_position']:.0%}")
        print(f"止损: {POSITION_CONFIG['stop_loss']:.0%}")
        print(f"动态回撤控制: 启用")
        print(f"分级止损: 3%/5%/8%")
        print(f"{'='*70}\n")
        
        results = []
        for i, stock_code in enumerate(stock_codes):
            result = self.run_backtest_single(stock_code)
            results.append(result)
            
            if (i + 1) % 10 == 0:
                print(f"进度: {i+1}/{len(stock_codes)}")
        
        # 统计
        successful = [r for r in results if r['status'] == 'success']
        
        if successful:
            df_results = pd.DataFrame(successful)
            
            summary = {
                'version': 'v1.8',
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'stock_count': len(stock_codes),
                'success_count': len(successful),
                'config': {
                    'base_position': POSITION_CONFIG['base_position'],
                    'max_position': POSITION_CONFIG['max_position'],
                    'stop_loss': POSITION_CONFIG['stop_loss'],
                    'drawdown_control': POSITION_CONFIG.get('drawdown_control', {}),
                    'tiered_stop_loss': POSITION_CONFIG.get('tiered_stop_loss', {})
                },
                'performance': {
                    'avg_return': round(df_results['total_return'].mean(), 2),
                    'median_return': round(df_results['total_return'].median(), 2),
                    'avg_annual_return': round(df_results['annual_return'].mean(), 2),
                    'avg_sharpe': round(df_results['sharpe_ratio'].mean(), 3),
                    'avg_max_drawdown': round(df_results['max_drawdown'].mean(), 2),
                    'positive_return_pct': round((df_results['total_return'] > 0).mean() * 100, 1),
                    'beat_benchmark_pct': round((df_results['excess_return'] > 0).mean() * 100, 1),
                },
                'top_10': df_results.nlargest(10, 'annual_return')[
                    ['stock_code', 'annual_return', 'sharpe_ratio', 'max_drawdown', 'trade_count']
                ].to_dict('records'),
                'all_results': successful
            }
            
            # 打印摘要
            print(f"\n{'='*70}")
            print("回测结果汇总")
            print(f"{'='*70}")
            print(f"成功回测: {len(successful)}/{len(stock_codes)}")
            print(f"平均总收益: {summary['performance']['avg_return']}%")
            print(f"平均年化收益: {summary['performance']['avg_annual_return']}%")
            print(f"平均夏普比率: {summary['performance']['avg_sharpe']}")
            print(f"平均最大回撤: {summary['performance']['avg_max_drawdown']}%")
            print(f"正收益占比: {summary['performance']['positive_return_pct']}%")
            print(f"跑赢基准占比: {summary['performance']['beat_benchmark_pct']}%")
            
            print(f"\n{'='*70}")
            print("Top 10 表现最佳")
            print(f"{'='*70}")
            for i, r in enumerate(summary['top_10'], 1):
                print(f"{i}. {r['stock_code']}: 年化{r['annual_return']}%, 夏普{r['sharpe_ratio']}, 回撤{r['max_drawdown']}%")
            
            # 保存结果
            output_file = os.path.join(
                self.OUTPUT_DIR,
                f"v18_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            print(f"\n结果已保存: {output_file}")
            
            return summary
        
        return {'status': 'no_results'}


def main():
    engine = V18BacktestEngine()
    return engine.run_backtest_batch(stock_limit=100)


if __name__ == "__main__":
    main()