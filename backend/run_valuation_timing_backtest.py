"""
基于估值的动态仓位管理策略

核心逻辑：
1. 低估值（PE低、PB低、股息率高）时逐步加仓
2. 估值回升后逐步减仓
3. 分批建仓/减仓，平滑操作

估值指标：
- PE分位数：历史PE分位越低，估值越低
- PB分位数：历史PB分位越低，估值越低
- 股息率：股息率越高，估值越低
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import json
import tushare as ts

ts.set_token('21cbce2d06540b12e14765850fee73749ccfb0cd7570f466bf7d8e45')
pro = ts.pro_api()

DAY_CACHE_DIR = "data_cache/day"
OUTPUT_DIR = "data_cache/backtest_results"


class ValuationTimingStrategy:
    """估值择时策略"""
    
    # 仓位调整规则
    POSITION_RULES = {
        # 估值分位数区间 -> 目标仓位
        'extreme_low': {'pe_pct': 10, 'position': 0.90},   # 极度低估：加仓至90%
        'very_low': {'pe_pct': 20, 'position': 0.75},      # 非常低估：加仓至75%
        'low': {'pe_pct': 30, 'position': 0.60},           # 低估：加仓至60%
        'normal_low': {'pe_pct': 40, 'position': 0.50},    # 偏低：仓位50%
        'normal': {'pe_pct': 50, 'position': 0.40},        # 正常：仓位40%
        'normal_high': {'pe_pct': 60, 'position': 0.30},   # 偏高：减仓至30%
        'high': {'pe_pct': 70, 'position': 0.20},          # 高估：减仓至20%
        'very_high': {'pe_pct': 80, 'position': 0.10},     # 非常高估：减仓至10%
        'extreme_high': {'pe_pct': 100, 'position': 0.05}, # 极度高估：轻仓5%
    }
    
    # 分批调整步长（避免一次性大幅调仓）
    MAX_POSITION_CHANGE = 0.10  # 单次最大调整10%
    
    def __init__(self, initial_capital: float = 1000000):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        self.initial_capital = initial_capital
    
    def load_index_data(self) -> pd.DataFrame:
        """加载指数数据（沪深300或上证指数）"""
        # 尝试加载上证指数
        file_path = os.path.join(DAY_CACHE_DIR, "000001_day.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, encoding='utf-8')
            if '日期' in df.columns:
                df['trade_date'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
            if '收盘' in df.columns:
                df['close'] = df['收盘']
            df = df.dropna(subset=['trade_date', 'close'])
            df = df.sort_values('trade_date').reset_index(drop=True)
            return df
        
        # 从Tushare获取
        try:
            df = pro.index_daily(ts_code='000001.SH', start_date='20160101', end_date='20260322')
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df = df.sort_values('trade_date').reset_index(drop=True)
            return df[['trade_date', 'close', 'pe', 'pe_ttm', 'pb']]
        except:
            return pd.DataFrame()
    
    def calculate_valuation_percentile(self, df: pd.DataFrame, window: int = 252*3) -> pd.DataFrame:
        """计算估值分位数"""
        result = df.copy()
        
        # 如果没有PE数据，用价格代理
        if 'pe_ttm' not in result.columns:
            # 使用价格的倒数作为PE代理（简化）
            result['pe_proxy'] = 1 / result['close'] * 100
            pe_col = 'pe_proxy'
        else:
            pe_col = 'pe_ttm'
        
        # 计算PE分位数（过去3年）
        result['pe_percentile'] = result[pe_col].rolling(window, min_periods=100).apply(
            lambda x: (x.iloc[-1] < x[:-1]).mean() * 100 if len(x) > 30 else 50
        )
        
        # 计算PB分位数
        if 'pb' in result.columns:
            result['pb_percentile'] = result['pb'].rolling(window, min_periods=100).apply(
                lambda x: (x.iloc[-1] < x[:-1]).mean() * 100 if len(x) > 30 else 50
            )
        else:
            result['pb_percentile'] = 50
        
        # 综合估值分位数
        result['valuation_pct'] = (
            result['pe_percentile'].fillna(50) * 0.5 +
            result['pb_percentile'].fillna(50) * 0.3 +
            50 * 0.2  # 默认中性
        )
        
        return result
    
    def get_target_position(self, valuation_pct: float) -> float:
        """根据估值分位数获取目标仓位"""
        for rule_name, rule in self.POSITION_RULES.items():
            if valuation_pct <= rule['pe_pct']:
                return rule['position']
        return 0.05
    
    def run_backtest(self) -> Dict:
        """运行回测"""
        print("=" * 70)
        print("估值择时策略回测")
        print("=" * 70)
        
        # 加载数据
        df = self.load_index_data()
        if df.empty:
            print("无法加载数据")
            return {}
        
        print(f"数据范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")
        print(f"数据条数: {len(df)}")
        
        # 计算估值分位数
        df = self.calculate_valuation_percentile(df)
        
        # 初始化
        cash = self.initial_capital
        position = 0  # 持有份额
        target_position_pct = 0.40  # 初始仓位40%
        current_position_pct = 0
        
        trades = []
        portfolio_values = []
        
        # 遍历每个交易日
        for i in range(252, len(df)):  # 跳过第一年（用于计算分位数）
            date = df['trade_date'].iloc[i]
            price = df['close'].iloc[i]
            valuation_pct = df['valuation_pct'].iloc[i]
            
            # 计算当前组合价值
            current_value = cash + position * price
            
            # 根据估值获取目标仓位
            target_position_pct = self.get_target_position(valuation_pct)
            
            # 分批调整（避免一次性大幅调仓）
            position_diff = target_position_pct - current_position_pct
            if abs(position_diff) > self.MAX_POSITION_CHANGE:
                # 分步调整
                if position_diff > 0:
                    adjusted_position_pct = current_position_pct + self.MAX_POSITION_CHANGE
                else:
                    adjusted_position_pct = current_position_pct - self.MAX_POSITION_CHANGE
            else:
                adjusted_position_pct = target_position_pct
            
            # 计算需要调整的金额
            target_value = current_value * adjusted_position_pct
            current_position_value = position * price
            trade_value = target_value - current_position_value
            
            # 执行交易
            if trade_value > 1000:  # 加仓，最小1千元
                # 买入
                shares = trade_value / price
                cost = shares * price * 1.0003  # 手续费
                if cost <= cash:
                    position += shares
                    cash -= cost
                    trades.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'action': 'buy',
                        'shares': shares,
                        'price': price,
                        'value': trade_value,
                        'valuation_pct': valuation_pct,
                        'position_pct': adjusted_position_pct
                    })
                    current_position_pct = adjusted_position_pct
                    
            elif trade_value < -1000:  # 减仓
                # 卖出
                shares = min(-trade_value / price, position)
                if shares > 0:
                    revenue = shares * price * 0.9997
                    position -= shares
                    cash += revenue
                    trades.append({
                        'date': date.strftime('%Y-%m-%d'),
                        'action': 'sell',
                        'shares': shares,
                        'price': price,
                        'value': -trade_value,
                        'valuation_pct': valuation_pct,
                        'position_pct': adjusted_position_pct
                    })
                    current_position_pct = adjusted_position_pct
            
            # 记录组合价值
            portfolio_values.append({
                'date': date,
                'value': cash + position * price,
                'cash': cash,
                'position_value': position * price,
                'position_pct': current_position_pct,
                'valuation_pct': valuation_pct,
                'price': price
            })
        
        # 计算业绩
        portfolio_df = pd.DataFrame(portfolio_values)
        portfolio_df['returns'] = portfolio_df['value'].pct_change()
        
        total_return = portfolio_df['value'].iloc[-1] / self.initial_capital - 1
        years = len(portfolio_df) / 252
        annual_return = (1 + total_return) ** (1 / years) - 1
        
        # 夏普比率
        sharpe = portfolio_df['returns'].mean() / portfolio_df['returns'].std() * np.sqrt(252)
        
        # 最大回撤
        cummax = portfolio_df['value'].cummax()
        drawdown = (portfolio_df['value'] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        # 基准收益（买入持有）
        benchmark_return = df['close'].iloc[-1] / df['close'].iloc[252] - 1
        
        # 年度收益
        portfolio_df['year'] = portfolio_df['date'].dt.year
        yearly_returns = portfolio_df.groupby('year').apply(
            lambda x: (1 + x['returns']).prod() - 1
        ).to_dict()
        
        # 打印结果
        print(f"\n{'='*70}")
        print("📊 回测结果")
        print(f"{'='*70}")
        
        print(f"\n【策略收益】")
        print(f"  总收益: {total_return*100:.2f}%")
        print(f"  年化收益: {annual_return*100:.2f}%")
        print(f"  夏普比率: {sharpe:.3f}")
        print(f"  最大回撤: {max_drawdown*100:.2f}%")
        
        print(f"\n【基准收益（买入持有）】")
        print(f"  总收益: {benchmark_return*100:.2f}%")
        print(f"  年化收益: {(1+benchmark_return)**(1/years)-1:.2%}")
        
        print(f"\n【超额收益】")
        print(f"  总超额: {(total_return - benchmark_return)*100:.2f}%")
        
        print(f"\n【年度收益】")
        for year, ret in yearly_returns.items():
            print(f"  {year}: {ret*100:.2f}%")
        
        print(f"\n【交易统计】")
        buy_trades = [t for t in trades if t['action'] == 'buy']
        sell_trades = [t for t in trades if t['action'] == 'sell']
        print(f"  总交易次数: {len(trades)}")
        print(f"  买入次数: {len(buy_trades)}")
        print(f"  卖出次数: {len(sell_trades)}")
        
        # 仓位分布
        print(f"\n【仓位分布】")
        position_bins = [0, 0.2, 0.4, 0.6, 0.8, 1.0]
        portfolio_df['position_bin'] = pd.cut(portfolio_df['position_pct'], bins=position_bins)
        position_dist = portfolio_df['position_bin'].value_counts()
        for bin_label, count in position_dist.items():
            pct = count / len(portfolio_df) * 100
            print(f"  {bin_label}: {pct:.1f}%")
        
        # 保存结果
        result = {
            'strategy': '估值择时策略',
            'backtest_period': {
                'start': portfolio_df['date'].min().strftime('%Y-%m-%d'),
                'end': portfolio_df['date'].max().strftime('%Y-%m-%d'),
                'years': round(years, 1)
            },
            'performance': {
                'total_return': f"{total_return*100:.2f}%",
                'annual_return': f"{annual_return*100:.2f}%",
                'sharpe_ratio': round(sharpe, 3),
                'max_drawdown': f"{max_drawdown*100:.2f}%",
            },
            'benchmark': {
                'total_return': f"{benchmark_return*100:.2f}%",
                'annual_return': f"{(1+benchmark_return)**(1/years)-1:.2%}",
            },
            'excess_return': f"{(total_return - benchmark_return)*100:.2f}%",
            'yearly_returns': {str(k): f"{v*100:.2f}%" for k, v in yearly_returns.items()},
            'trade_stats': {
                'total_trades': len(trades),
                'buy_trades': len(buy_trades),
                'sell_trades': len(sell_trades),
            },
            'final_value': round(portfolio_df['value'].iloc[-1], 2)
        }
        
        output_file = os.path.join(
            OUTPUT_DIR,
            f"valuation_timing_backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"\n结果已保存: {output_file}")
        
        return result


def main():
    strategy = ValuationTimingStrategy(initial_capital=1000000)
    return strategy.run_backtest()


if __name__ == '__main__':
    main()