#!/usr/bin/env python
"""
CTA策略回测示例

策略类型: 趋势跟踪
策略名称: 双均线策略 + 唐奇安通道突破 + 海龟策略
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.insert(0, '.')

print("=" * 70)
print("CTA策略回测示例")
print("=" * 70)

# ============================================================
# 1. 数据加载
# ============================================================
print("\n[1] 加载日线数据...")

DATA_DIR = "data_cache/day"

def load_stock_data(code):
    """加载单只股票数据"""
    file_path = os.path.join(DATA_DIR, f"{code}_day.csv")
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        if '日期' in df.columns:
            df['日期'] = pd.to_datetime(df['日期'].astype(str))
            df = df.sort_values('日期').reset_index(drop=True)
            return df
    return None

# 加载平安银行作为示例
df = load_stock_data('000001')
if df is None:
    print("数据加载失败")
    sys.exit(1)

print(f"  平安银行(000001): {len(df)}条数据")
print(f"  日期范围: {df['日期'].iloc[0]} ~ {df['日期'].iloc[-1]}")

# ============================================================
# 2. 策略定义
# ============================================================
print("\n[2] 定义CTA策略...")

class DualMAStrategy:
    """双均线策略"""
    
    def __init__(self, short_period=20, long_period=60):
        self.short_period = short_period
        self.long_period = long_period
        self.name = f"双均线({short_period}/{long_period})"
    
    def generate_signals(self, df):
        """生成交易信号"""
        df = df.copy()
        df['MA_Short'] = df['收盘'].rolling(self.short_period).mean()
        df['MA_Long'] = df['收盘'].rolling(self.long_period).mean()
        df['Signal'] = 0
        df.loc[df['MA_Short'] > df['MA_Long'], 'Signal'] = 1
        df.loc[df['MA_Short'] < df['MA_Long'], 'Signal'] = -1
        return df


class DonchianChannelStrategy:
    """唐奇安通道突破策略"""
    
    def __init__(self, period=20):
        self.period = period
        self.name = f"唐奇安通道({period})"
    
    def generate_signals(self, df):
        """生成交易信号"""
        df = df.copy()
        df['Upper'] = df['最高'].rolling(self.period).max()
        df['Lower'] = df['最低'].rolling(self.period).min()
        df['Signal'] = 0
        df.loc[df['收盘'] >= df['Upper'].shift(1), 'Signal'] = 1
        df.loc[df['收盘'] <= df['Lower'].shift(1), 'Signal'] = -1
        return df


class TurtleStrategy:
    """海龟交易策略"""
    
    def __init__(self, entry_period=20, exit_period=10):
        self.entry_period = entry_period
        self.exit_period = exit_period
        self.name = f"海龟策略({entry_period}/{exit_period})"
    
    def generate_signals(self, df):
        """生成交易信号"""
        df = df.copy()
        df['Entry_High'] = df['最高'].rolling(self.entry_period).max()
        df['Entry_Low'] = df['最低'].rolling(self.entry_period).min()
        df['Exit_Low'] = df['最低'].rolling(self.exit_period).min()
        
        df['Signal'] = 0
        position = 0
        
        for i in range(len(df)):
            if position == 0:
                if df.loc[i, '收盘'] >= df.loc[i, 'Entry_High']:
                    position = 1
            elif position == 1:
                if df.loc[i, '收盘'] <= df.loc[i, 'Exit_Low']:
                    position = 0
            df.loc[i, 'Signal'] = position
        
        return df


# ============================================================
# 3. 回测引擎
# ============================================================
print("\n[3] 回测引擎...")

class CTABacktester:
    """CTA回测引擎"""
    
    def __init__(self, initial_capital=100000, commission=0.0003):
        self.initial_capital = initial_capital
        self.commission = commission
    
    def run(self, df, strategy):
        """运行回测"""
        df = strategy.generate_signals(df)
        df['Returns'] = df['收盘'].pct_change()
        df['Strategy_Returns'] = df['Signal'].shift(1) * df['Returns']
        df['Position_Change'] = df['Signal'].diff().abs()
        df['Commission'] = df['Position_Change'] * self.commission
        df['Strategy_Returns'] = df['Strategy_Returns'] - df['Commission']
        df['Cumulative_Returns'] = (1 + df['Returns']).cumprod()
        df['Cumulative_Strategy'] = (1 + df['Strategy_Returns']).cumprod()
        
        results = self._calculate_metrics(df)
        results['strategy_name'] = strategy.name
        results['df'] = df
        return results
    
    def _calculate_metrics(self, df):
        """计算回测指标"""
        returns = df['Strategy_Returns'].dropna()
        total_return = df['Cumulative_Strategy'].iloc[-1] - 1
        years = len(df) / 252
        annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        cumulative = df['Cumulative_Strategy']
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        
        sharpe = (returns.mean() * 252 - 0.03) / (returns.std() * np.sqrt(252)) if returns.std() > 0 else 0
        win_rate = (returns > 0).sum() / len(returns) if len(returns) > 0 else 0
        avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0
        avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 1
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0
        trades = df['Position_Change'].sum() / 2
        
        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio,
            'trades': trades,
        }


# ============================================================
# 4. 运行回测
# ============================================================
print("\n[4] 运行回测...")

backtester = CTABacktester(initial_capital=100000, commission=0.0003)

strategies = [
    DualMAStrategy(20, 60),
    DualMAStrategy(10, 30),
    DonchianChannelStrategy(20),
    DonchianChannelStrategy(40),
    TurtleStrategy(20, 10),
]

results_list = []
for strategy in strategies:
    print(f"  回测 {strategy.name}...")
    results = backtester.run(df, strategy)
    results_list.append(results)

# ============================================================
# 5. 输出结果
# ============================================================
print("\n" + "=" * 70)
print("回测结果对比")
print("=" * 70)

print(f"\n{'策略':<20} {'年化收益':>10} {'最大回撤':>10} {'夏普比率':>10} {'胜率':>8} {'交易次数':>8}")
print("-" * 70)

for r in results_list:
    print(f"{r['strategy_name']:<20} "
          f"{r['annual_return']*100:>9.2f}% "
          f"{r['max_drawdown']*100:>9.2f}% "
          f"{r['sharpe_ratio']:>10.2f} "
          f"{r['win_rate']*100:>7.2f}% "
          f"{r['trades']:>8.0f}")

benchmark_return = (df['收盘'].iloc[-1] / df['收盘'].iloc[0] - 1)
years = len(df) / 252
benchmark_annual = (1 + benchmark_return) ** (1 / years) - 1 if years > 0 else 0
print("-" * 70)
print(f"{'基准(买入持有)':<20} {benchmark_annual*100:>9.2f}%")

best = max(results_list, key=lambda x: x['sharpe_ratio'])
print(f"\n最佳策略: {best['strategy_name']}")
print("-" * 70)
print(f"  总收益率: {best['total_return']*100:.2f}%")
print(f"  年化收益: {best['annual_return']*100:.2f}%")
print(f"  最大回撤: {best['max_drawdown']*100:.2f}%")
print(f"  夏普比率: {best['sharpe_ratio']:.2f}")
print(f"  胜率:     {best['win_rate']*100:.2f}%")
print(f"  盈亏比:   {best['profit_loss_ratio']:.2f}")

OUTPUT_DIR = "data_cache/backtest_results"
os.makedirs(OUTPUT_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = os.path.join(OUTPUT_DIR, f"cta_backtest_{timestamp}.csv")
results_df = pd.DataFrame([{k: v for k, v in r.items() if k != 'df'} for r in results_list])
results_df.to_csv(output_file, index=False)
print(f"\n回测结果已保存: {output_file}")
print("\n" + "=" * 70)